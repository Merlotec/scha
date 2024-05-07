use atomic::run_atomic;
use csv::Writer;
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, error::Error, io, os::windows::raw::SOCKET, path::Path, process};

pub mod atomic;

pub const LADs: [&'static str; 34] = [
    "Blackburn with Darwen",
    "Blackpool",
    "Bolton",
    "Burnley",
    "Bury",
    "Cheshire East",
    "Cheshire West and Chester",
    "Cumberland",
    "Fylde",
    "Halton",
    "Hyndburn",
    "Knowsley",
    "Lancaster",
    "Liverpool",
    "Manchester",
    "Oldham",
    "Pendle",
    "Preston",
    "Ribble Valley",
    "Rochdale",
    "Rossendale",
    "Salford",
    "Sefton",
    "South Ribble",
    "St Helens",
    "Stockport",
    "Tameside",
    "Trafford",
    "Warrington",
    "West Lancashire",
    "Westmorland and Furness",
    "Wigan",
    "Wirral",
    "Wyre",
];

pub const TARGET_SCHOOL_TYPES: [&'static str; 12] = [
    "AC",
    "ACC",
    "AC1619",
    "ACC1619",
    "CY",
    "F1619",
    "FSS",
    "F",
    "FD",
    "FD",
    "VA",
    "VC",
];

pub const CUM_RPI_DEFL: [f32; 7] = [
    1.0, //2017
    1.036, // 2018 : base * 2017 rpi
    1.070188,
    1.09801288,
    1.114483081,
    1.159062405,
    1.293513644,
];  

pub struct Scaler {
    vals: Vec<(f32, f32)>,
}

impl Scaler {
    pub fn new() -> Self {
        Self { vals: Vec::new() }
    }

    pub fn add(&mut self, v: f32, w: f32) {
        if w > 0.0 {
            self.vals.push((v, w));
        }
    }

    pub fn ave(&self) -> Option<f32> {
        if self.vals.is_empty() {
            None
        } else {
            let sum: f32 = self.vals.iter().map(|v| v.1).sum();

            let mut x = 0.0;
            for (v, w) in self.vals.iter() {
                x += v * (w / &sum);
            }
            Some(x)
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SchoolRecord {
    #[serde(rename = "TOWN")]
    town: String,
    #[serde(rename = "PCODE")]
    pcode: String,
    #[serde(rename = "SCHNAME")]
    name: String,
    #[serde(rename = "NFTYPE")]
    school_type: String,
    #[serde(rename = "ADMPOL")]
    adm_pol: String,
    #[serde(rename = "URN")]
    urn: String,
    #[serde(rename = "TOTPUPS")]
    pop: String,
    #[serde(rename = "P8MEA")]
    p8: String,
    #[serde(rename = "P8MEAEBAC")]
    ebacc: String,
    #[serde(rename = "PTL2BASICS_94")]
    gcseg2: String,
    #[serde(rename = "PTFSM6CLA1ABASICS_94")]
    gcseg2_dis: String,
}

impl School for SchoolRecord {
    fn get_urn(&self) -> &str {
        &self.urn
    }

    fn get_pcode(&self) -> &str {
        &self.pcode
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PSchoolRecord {
    #[serde(rename = "TOWN")]
    town: String,
    #[serde(rename = "PCODE")]
    pcode: String,
    #[serde(rename = "SCHNAME")]
    name: String,
    #[serde(rename = "NFTYPE")]
    school_type: String,
    #[serde(rename = "URN")]
    urn: String,
    #[serde(rename = "TOTPUPS")]
    pop: String,
    #[serde(rename = "PTRWM_EXP")]
    rwm_ta: String,
    #[serde(rename = "PTRWM_EXP_FSM6CLA1A")]
    rwm_ta_dis: String,
}

impl School for PSchoolRecord {
    fn get_urn(&self) -> &str {
        &self.urn
    }

    fn get_pcode(&self) -> &str {
        &self.pcode
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RegionPcodeRecord {
    #[serde(rename = "pcd")]
    pcode: String,
    #[serde(rename = "lad23cd")]
    lad_code: String,
    #[serde(rename = "lad23nm")]
    lad: String,
}

struct SchoolInfo<S: School> {
    record: S,
    ofsted: Option<OfstedRecord>,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct AggregateRecord {
    year: String,
    lad: String,
    n: u32,
    n_valid: u32,
    score: Option<f32>,
    binary_weighted_p8: Option<f32>,
    weighted_p8: Option<f32>,
    gsceg2_ag: Option<f32>,
    gcseg2_dis_ag: Option<f32>,
    of_overall_ag: Option<f32>,
    of_educ_ag: Option<f32>,
    of_behaviour_ag: Option<f32>,
    of_pdev_ag: Option<f32>,
    of_sixthform_ag: Option<f32>,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct AggregatePRecord {
    year: String,
    lad: String,
    n: u32,
    n_valid: u32,
    of_overall_ag: Option<f32>,
    score: Option<f32>,
    of_educ_ag: Option<f32>,
    of_behaviour_ag: Option<f32>,
    of_pdev_ag: Option<f32>,
    rwm_ta_ag: Option<f32>,
    rwm_ta_dis_ag: Option<f32>,
}

impl AggregatePRecord {
    pub fn empty(year: String, lad: String) -> Self {
        Self {
            year,
            lad,
            ..Default::default()
        }
    }
}



impl AggregateRecord {
    pub fn empty(year: String, lad: String) -> Self {
        Self {
            year,
            lad,
            ..Default::default()
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct AggregateSchoolRecord {
    pub year: u32,
    pub lad: String,
    pub name: String,
    pub pcode: String,
    pub urn: String,
    pub school_type: String,
    pub is_state: u32,
    pub is_selective: u32,
    pub p8: String,
    pub ebacc: String,
    pub score: Option<f32>,
    pub sc_p8: Option<f32>,
    pub of_overall: Option<u32>,
    pub of_educ: Option<u32>,
    pub of_behaviour: Option<u32>,
    pub of_pdev: Option<u32>,
    pub of_sixthform: Option<u32>,
    pub gcseg2: Option<f32>,
    pub gcseg2_dis: Option<f32>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct AggregatePSchoolRecord {
    pub year: u32,
    pub lad: String,
    pub name: String,
    pub pcode: String,
    pub urn: String,
    pub school_type: String,
    pub is_state: u32,
    pub score: Option<f32>,
    pub rwm_ta: Option<f32>,
    pub rwm_ta_dis: Option<f32>,
    pub of_overall: Option<u32>,
    pub of_educ: Option<u32>,
    pub of_behaviour: Option<u32>,
    pub of_pdev: Option<u32>,
}


trait School {
    fn get_urn(&self) -> &str;

    fn get_pcode(&self) -> &str;
}

fn load_regions<P: AsRef<Path>>(path: P) -> Result<HashMap<String, String>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path(path)?;

    let mut iter = rdr.deserialize::<RegionPcodeRecord>();

    let mut region_map: HashMap<String, String> = HashMap::new();
    for result in iter {
        if let Ok(record) = result {
            let mut lad = record.lad;
            lad.replace(".", "");
            if LADs.contains(&lad.as_str()) {
                region_map.insert(record.pcode.trim().to_owned(), lad.clone());
            }
        }
    }

    Ok(region_map)
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct OfstedRecord {
    #[serde(rename = "URN")]
    urn: String,

    #[serde(rename = "Local authority")]
    lad: String,

    #[serde(rename = "Overall effectiveness")]
    overall: Option<u32>,

    #[serde(rename = "Quality of education")]
    educ: Option<u32>,

    #[serde(rename = "Behaviour and attitudes")]
    behaviour: Option<u32>,

    #[serde(rename = "Personal development")]
    pdev: Option<u32>,

    #[serde(rename = "Sixth form provision (where applicable)")]
    sixthform: Option<u32>,
}

fn find_ofsted<P: AsRef<Path>>(path: P, urn: &str) -> Result<Option<OfstedRecord>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let mut iter = rdr.deserialize::<OfstedRecord>();

    let mut region_map: HashMap<String, String> = HashMap::new();

    for result in iter {
        if let Ok(record) = result {
            if &record.urn == urn {
                return Ok(Some(record));
            }
        }
    }

    Ok(None)
}

fn load_ofsted<P: AsRef<Path>>(path: P) -> Result<HashMap<String, OfstedRecord>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let mut iter = rdr.deserialize::<OfstedRecord>();

    let mut map: HashMap<String, OfstedRecord> = HashMap::new();

    for result in iter {
        if let Ok(record) = result {
            map.insert(record.urn.clone(), record);
        }
    }

    Ok(map)
}

fn first_letters(postcode: &str) -> Option<String> {
    let re = Regex::new(r"^[A-Za-z]+").unwrap();
    match re.find(postcode) {
        Some(matched) => Some(matched.as_str().trim().to_string()),
        None => None,
    }
}

fn parse_dset<P: AsRef<Path>, S: School + DeserializeOwned>(
    path: P,
    ofsted_data: &HashMap<String, OfstedRecord>,
    region_map: &HashMap<String, String>,
) -> Result<HashMap<String, Vec<SchoolInfo<S>>>, Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        //.has_headers(true)
        //.flexible(true)
        .from_path(path)?;

    let mut iter = rdr.deserialize::<S>();

    let mut schools: HashMap<String, Vec<SchoolInfo<S>>> = HashMap::new();

    let mut man_n = 0;
    let mut man_a = 0;

    let mut failed = 0;

    for result in iter {
        match result {
            Ok(record) => {
                let ofsted = ofsted_data.get(record.get_urn()).cloned();
                    //if let Some(mut lad) = region_map.get(record.pcode.trim()).cloned() {

                    let mut lad: String = String::new();

                    if let Some(ofsted) = &ofsted {
                        lad = ofsted.lad.clone();
                    } 
                    if !LADs.contains(&lad.as_str()) {
                        if let Some(l) = region_map.get(record.get_pcode().trim()).cloned() {
                            lad = l;
                        }
                    }

                    if LADs.contains(&lad.as_str()) {
                        // Get ofsted data for school.
                        if let Some(ofsted) = &ofsted {
                            if LADs.contains(&ofsted.lad.as_str()) {
                                lad = ofsted.lad.clone();
                            }
                        }
                        let info = SchoolInfo { record, ofsted };
                        if let Some(v) = schools.get_mut(&lad) {
                            v.push(info);
                        } else {
                            schools.insert(lad, vec![info]);
                        }
                    }
            }
            Err(e) => {
                println!("{}", e);
                failed += 1;
            }
        }
    }

    //println!("a, n, f: {}, {}, {}", man_a, man_n, failed);
    Ok(schools)
}

fn percentage_string_to_float(input: &str) -> Result<f32, std::num::ParseFloatError> {
    let cleaned = input.trim_end_matches('%');
    cleaned.parse::<f32>().map(|n| n / 100.0)
}

const W_P8: f32 = 1.3;
const W_EBAC: f32 = 1.3;
const SC_P8_BASE: f32 = 0.5;

fn aggregate_sec(
    schools: &[SchoolInfo<SchoolRecord>],
    year: u32,
    lad: String,
) -> Option<(AggregateRecord, Vec<AggregateSchoolRecord>)> {
    if !schools.is_empty() {
        let mut sum = 0.0;
        let mut binary_weighted_p8 = 0.0;
        let mut weighted_p8 = Scaler::new();
        let mut records = Vec::new();

        let mut gsce2_ag = Scaler::new();
        let mut gsce2_dis_ag = Scaler::new();


        let mut of_overall_ag = Scaler::new();
        let mut of_behaviour_ag = Scaler::new();
        let mut of_educ_ag = Scaler::new();
        let mut of_pdev_ag = Scaler::new();
        let mut of_sixthform_ag = Scaler::new();

        let mut n_valid = 0;

        let total_pop: u32 = schools
            .iter()
            .map(|x| x.record.pop.parse::<u32>().unwrap_or(500))
            .sum();

        for school in schools {
            let mut components = Vec::new();

            let mut good_sch: bool = false;

            let mut sc_p8: Option<f32> = None;

            let pop = school.record.pop.parse::<u32>().unwrap_or(500);
            let w = pop as f32 / total_pop as f32;

            let gcseg2 = percentage_string_to_float(&school.record.gcseg2).ok();
            let gcseg2_dis = percentage_string_to_float(&school.record.gcseg2_dis).ok();

            let selective = school.record.adm_pol == "SEL";

            // Only choose the right kind of schools.
            let state = TARGET_SCHOOL_TYPES.contains(&school.record.school_type.as_str()) && !selective;
            if state && !selective {
                n_valid += 1;
                if let Some(x) = gcseg2 {
                    gsce2_ag.add(x, w);
                }

                if let Some(x) = gcseg2_dis {
                    gsce2_dis_ag.add(x, w);
                }

                if let Some(of) = school.ofsted.as_ref().and_then(|o| o.overall) {
                    of_overall_ag.add(of as f32, w);
                }
                if let Some(of) = school.ofsted.as_ref().and_then(|o| o.behaviour) {
                    of_behaviour_ag.add(of as f32, w);
                }
                if let Some(of) = school.ofsted.as_ref().and_then(|o| o.educ) {
                    of_educ_ag.add(of as f32, w);
                }
                if let Some(of) = school.ofsted.as_ref().and_then(|o| o.pdev) {
                    of_pdev_ag.add(of as f32, w);
                }
                if let Some(of) = school.ofsted.as_ref().and_then(|o| o.sixthform) {
                    of_sixthform_ag.add(of as f32, w);
                }
    
                if let Ok(x) = school.record.p8.parse::<f32>() {
                    weighted_p8.add(x, w);
                    components.push(x);
                    good_sch = x >= 0.0;
    
                    if good_sch {
                        if let Ok(pop) = school.record.pop.parse::<u32>() {
                            let p = (SC_P8_BASE + x) * w;
                            sc_p8 = Some(p);
                            binary_weighted_p8 += p;
                        }
                    }
                }
    
                if let Some(x) = school.ofsted.as_ref().and_then(|s| s.overall) {
                    components.push((x as f32 * 0.1) - 1.0);
                }
                // if let Ok(x) = percentage_string_to_float(&school.gcseg5) {
                //     components.push(x);
                // }
                if let Ok(x) = school.record.ebacc.parse::<f32>() {
                    components.push((x));
                }
            }            

            let mut sc: Option<f32> = None;
            if !components.is_empty() {
                let score = components.iter().sum::<f32>();
                let s = score / components.len() as f32;
                sum += s;
                sc = Some(s);
            } else {
                //println!("EMPTY SCHOOL: {}, {}", &school.ebacc, &school.p8);
            }

            records.push(AggregateSchoolRecord {
                year,
                name: school.record.name.clone(),
                pcode: school.record.pcode.clone(),
                urn: school.record.urn.clone(),
                is_selective: selective as u32,
                school_type: school.record.school_type.clone(),
                is_state: state as u32,
                lad: lad.clone(),
                ebacc: school.record.ebacc.clone(),
                p8: school.record.p8.clone(),
                score: sc,
                sc_p8,
                of_overall: school.ofsted.as_ref().and_then(|x| x.overall),
                of_behaviour: school.ofsted.as_ref().and_then(|x| x.behaviour),
                of_educ: school.ofsted.as_ref().and_then(|x| x.educ),
                of_pdev: school.ofsted.as_ref().and_then(|x| x.pdev),
                of_sixthform: school.ofsted.as_ref().and_then(|x| x.sixthform),

                gcseg2,
                gcseg2_dis,
            })
        }
        let ave = sum / schools.len() as f32;

        Some((
            AggregateRecord {
                lad,
                n: schools.len() as u32,
                score: Some(ave),
                year: year.to_string(),
                binary_weighted_p8: Some(binary_weighted_p8),
                weighted_p8: weighted_p8.ave(),
                of_overall_ag: of_overall_ag.ave(),
                of_behaviour_ag: of_behaviour_ag.ave(),
                of_educ_ag: of_educ_ag.ave(),
                of_pdev_ag: of_pdev_ag.ave(),
                of_sixthform_ag: of_sixthform_ag.ave(),
                n_valid,
                gsceg2_ag: gsce2_ag.ave(),
                gcseg2_dis_ag: gsce2_dis_ag.ave(),
            },
            records,
        ))
    } else {
        None
    }
}


fn aggregate_prim(
    schools: &[SchoolInfo<PSchoolRecord>],
    year: u32,
    lad: String,
) -> Option<(AggregatePRecord, Vec<AggregatePSchoolRecord>)> {
    if !schools.is_empty() {
        let mut records = Vec::new();
        let mut sum = 0.0;
        let mut of_overall_ag = Scaler::new();
        let mut of_behaviour_ag = Scaler::new();
        let mut of_educ_ag = Scaler::new();
        let mut of_pdev_ag = Scaler::new();

        let mut rwm_ta_ag = Scaler::new();
        let mut rwm_ta_dis_ag = Scaler::new();

        let mut n_valid: u32 = 0;

        let total_pop: u32 = schools
            .iter()
            .map(|x| x.record.pop.parse::<u32>().unwrap_or(500))
            .sum();

        for school in schools {
            let mut components = Vec::new();
            let pop = school.record.pop.parse::<u32>().unwrap_or(500);
            let w = pop as f32 / total_pop as f32;

            let rwm_ta = percentage_string_to_float(&school.record.rwm_ta).ok();
            let rwm_ta_dis = percentage_string_to_float(&school.record.rwm_ta_dis).ok();
            let state = TARGET_SCHOOL_TYPES.contains(&school.record.school_type.as_str());
            if state {
                n_valid += 1;
                if let Some(x) = rwm_ta {
                    rwm_ta_ag.add(x, w);
                }

                if let Some(x) = rwm_ta_dis {
                    rwm_ta_dis_ag.add(x, w);
                }

                if let Some(of) = school.ofsted.as_ref().and_then(|o| o.overall) {
                    of_overall_ag.add(of as f32, w);
                }
                if let Some(of) = school.ofsted.as_ref().and_then(|o| o.behaviour) {
                    of_behaviour_ag.add(of as f32, w);
                }
                if let Some(of) = school.ofsted.as_ref().and_then(|o| o.educ) {
                    of_educ_ag.add(of as f32, w);
                }
                if let Some(of) = school.ofsted.as_ref().and_then(|o| o.pdev) {
                    of_pdev_ag.add(of as f32, w);
                }

                if let Some(x) = school.ofsted.as_ref().and_then(|s| s.overall) {
                    components.push((x as f32 * 0.1) - 1.0);
                }
                // if let Ok(x) = percentage_string_to_float(&school.gcseg5) {
                //     components.push(x);
                // }
                if let Some(x) = school.ofsted.as_ref().and_then(|s| s.educ) {
                    components.push((x as f32 * 0.1) - 1.0);
                }

                if let Some(x) = school.ofsted.as_ref().and_then(|s| s.behaviour) {
                    components.push((x as f32 * 0.1) - 1.0);
                }
            }
            let mut sc: Option<f32> = None;
            if !components.is_empty() {
                let score = components.iter().sum::<f32>();
                let s = score / components.len() as f32;
                sum += s;
                sc = Some(s);
            } else {
                //println!("EMPTY SCHOOL: {}, {}", &school.ebacc, &school.p8);
            }


            records.push(AggregatePSchoolRecord {
                year,
                name: school.record.name.clone(),
                pcode: school.record.pcode.clone(),
                urn: school.record.urn.clone(),
                is_state: state as u32,
                school_type: school.record.school_type.clone(),
                lad: lad.clone(),
                score: sc,
                of_overall: school.ofsted.as_ref().and_then(|x| x.overall),
                of_behaviour: school.ofsted.as_ref().and_then(|x| x.behaviour),
                of_educ: school.ofsted.as_ref().and_then(|x| x.educ),
                of_pdev: school.ofsted.as_ref().and_then(|x| x.pdev),
                rwm_ta,
                rwm_ta_dis,
            })
        }

        let ave = sum / schools.len() as f32;

        Some((
            AggregatePRecord {
                lad,
                n: schools.len() as u32,
                n_valid,
                year: year.to_string(),
                score: Some(ave),
                of_overall_ag: of_overall_ag.ave(),
                of_behaviour_ag: of_behaviour_ag.ave(),
                of_educ_ag: of_educ_ag.ave(),
                of_pdev_ag: of_pdev_ag.ave(),
                rwm_ta_ag: rwm_ta_ag.ave(),
                rwm_ta_dis_ag: rwm_ta_dis_ag.ave(),
            },
            records,
        ))
    } else {
        None
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    //run_schools()
    run_atomic()
    //combine_csv_files("depr", "depr.csv"); Ok(()) 
}

fn run_schools() -> Result<(), Box<dyn Error>> {
    let regions = load_regions("postcodes.csv")?;
    let ofsted = load_ofsted("ofsted.csv")?;

    println!("parsed postcodes, {}", regions.len());
    let mut agg_sec: Vec<(u32, Vec<AggregateRecord>)> = Vec::new();

    let mut agg_prim: Vec<(u32, Vec<AggregatePRecord>)> = Vec::new();

    let mut complete_writer_sec = Writer::from_path("scout_full_sec.csv")?;

    let mut complete_writer_prim = Writer::from_path("scout_full_prim.csv")?;

    for i in 2017..2024 {
        // let fname: String = format!("scraw_{}.csv", i);
        // sanitize(&fname, &format!("san_{}", &fname));

        // let fname: String = format!("scrawp_{}.csv", i);
        // sanitize(&fname, &format!("san_{}", &fname));
        // continue;
        {
            let fname = format!("san_scraw_{}.csv", i);
            let mut yr = Vec::new();

            match parse_dset(fname, &ofsted, &regions) {
                Ok(schools) => {
                    let mut keys: Vec<&str> = schools.keys().map(|k| k.as_str()).collect();
                    keys.sort();

                    for key in LADs {
                        if let Some(regional_schools) = &schools.get(key) {
                            if let Some((agg, records)) =
                                aggregate_sec(&regional_schools, i, key.to_owned())
                            {
                                yr.push(agg);

                                for rec in records {
                                    complete_writer_sec.serialize(&rec);
                                }
                            } else {
                                yr.push(AggregateRecord::empty(i.to_string(), key.to_owned()));
                                println!("No schools found for: {}", key);
                            }
                        } else {
                            println!("No entry found for: {}", key);
                            yr.push(AggregateRecord::empty(i.to_string(), key.to_owned()));
                        }
                        
                    }

                    agg_sec.push((i, yr));
                    println!("parsed schools {}", i);
                }
                Err(e) => println!("Failed to parse school: {}", e),
            }

        }
        // Primary
        {
            let fname = format!("san_scrawp_{}.csv", i);
            let mut yr = Vec::new();

            match parse_dset::<String, PSchoolRecord>(fname, &ofsted, &regions) {
                Ok(schools) => {
                    let mut keys: Vec<&str> = schools.keys().map(|k| k.as_str()).collect();
                    keys.sort();

                    for key in LADs {
                        if let Some(regional_schools) = &schools.get(key) {
                            if let Some((agg, records)) =
                                aggregate_prim(&regional_schools, i, key.to_owned())
                            {
                                yr.push(agg);

                                for rec in records {
                                    complete_writer_prim.serialize(&rec);
                                }
                            } else {
                                yr.push(AggregatePRecord::empty(i.to_string(), key.to_owned()));
                                println!("No schools found for: {}", key);
                            }
                        } else {
                            println!("No entry found for: {}", key);
                            yr.push(AggregatePRecord::empty(i.to_string(), key.to_owned()));
                        }
                        
                    }

                    agg_prim.push((i, yr));
                    println!("parsed schools {}", i);
                }
                Err(e) => println!("Failed to parse school: {}", e),
            }
        }
    }

    {
        let mut writer = Writer::from_path("scout_sec.csv")?;

        for (_, rows) in agg_sec {
            for row in rows {
                writer.serialize(&row);
            }
        }
    }
    {
        let mut writer = Writer::from_path("scout_prim.csv")?;

        for (_, rows) in agg_prim {
            for row in rows {
                writer.serialize(&row);
            }
        }
    }
    

    Ok(())
}

use csv::ReaderBuilder;

fn combine_csv_files(input_folder: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(output_file)?;
    let mut headers_written = false;

    for entry in std::fs::read_dir(input_folder)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            let mut reader = ReaderBuilder::new().has_headers(true).from_path(&path)?;
            if !headers_written {
                if let Ok(headers) = reader.headers() {
                    writer.write_record(headers)?;
                }
                headers_written = true;
            }

            for result in reader.records() {
                let record = result?;
                writer.write_record(&record)?;
            }
        }
    }

    writer.flush()?;
    Ok(())
}

fn sanitize<P: AsRef<Path>>(path: P, out: P) -> Result<(), Box<dyn Error>> {
    let file = std::fs::File::open(path)?;

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(file);
    let headers = rdr.headers()?.len();

    let mut writer = Writer::from_path(out)?;
    if let Ok(headers) = rdr.headers() {
        writer.write_record(headers)?;
    }

    for result in rdr.records() {
        if let Ok(mut record) = result {
            while record.len() < headers {
                record.push_field("");
            }

            writer.write_record(&record);
        }
    }

    Ok(())
}
