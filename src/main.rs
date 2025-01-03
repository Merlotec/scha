use atomic::run_atomic;
use csv::Writer;
use geo_rust::{Country, GeoLocation};
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, error::Error, io, path::Path, process};

pub mod assign;
pub mod atomic;
mod intersect;
mod render;

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

pub const TARGET_SCHOOL_TYPES: [&'static str; 11] = [
    "AC", "ACC", "AC1619", "ACC1619", "CY", "F1619", "FSS", "F", "FD", "VA", "VC",
];

pub const CUM_RPI_DEFL: [f32; 7] = [
    1.0,   //2017
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
    #[serde(rename = "msoa11")]
    msoa: String,
    #[serde(rename = "lat")]
    lat: String,
    #[serde(rename = "long")]
    long: String,
    #[serde(rename = "target_density")]
    target_density: String,
    #[serde(rename = "target_prop")]
    target_prop: String,
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
    #[serde(rename = "msoa11")]
    msoa: String,
    #[serde(rename = "lat")]
    lat: String,
    #[serde(rename = "long")]
    long: String,
    #[serde(rename = "target_density")]
    target_density: String,
    #[serde(rename = "target_prop")]
    target_prop: String,
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
    lad: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct AggregateRecord {
    year: String,
    lad: Option<String>,
    n: u32,
    n_valid: u32,
    score: Option<f32>,
    binary_weighted_p8: Option<f32>,
    weighted_p8: Option<f32>,
    gcseg2_ag: Option<f32>,
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
    pub fn empty(year: String, lad: Option<String>) -> Self {
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
    pub lad: Option<String>,
    pub msoa: String,
    pub name: String,
    pub pcode: String,
    pub lat: Option<f64>,
    pub lng: Option<f64>,
    pub x_km: Option<f64>,
    pub y_km: Option<f64>,
    pub radius: Option<f64>,
    pub target_density: Option<f64>,
    pub target_prop: Option<f64>,
    pub pop: Option<u32>,
    pub urn: String,
    pub school_type: String,
    pub is_state: u32,
    pub is_selective: u32,
    pub p8: String,
    pub ebacc: String,
    pub of_overall: Option<u32>,
    pub of_educ: Option<u32>,
    pub of_behaviour: Option<u32>,
    pub of_pdev: Option<u32>,
    pub of_sixthform: Option<u32>,
    pub gcseg2: Option<f32>,
    pub gcseg2_dis: Option<f32>,
}

impl AggregateSchoolRecord {
    #[inline]
    pub fn location(&self) -> Option<GeoLocation> {
        if let (Some(lat), Some(lng)) = (self.lat, self.lng) {
            Some(GeoLocation {
                latitude: lat,
                longitude: lng,
            })
        } else {
            None
        }
    }
}

impl AggregatePSchoolRecord {
    #[inline]
    pub fn location(&self) -> Option<GeoLocation> {
        if let (Some(lat), Some(lng)) = (self.lat, self.lng) {
            Some(GeoLocation {
                latitude: lat,
                longitude: lng,
            })
        } else {
            None
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct AggregatePSchoolRecord {
    pub year: u32,
    pub lad: Option<String>,
    pub name: String,
    pub pcode: String,
    pub lat: Option<f64>,
    pub lng: Option<f64>,
    pub pop: Option<u32>,
    pub x_km: Option<f64>,
    pub y_km: Option<f64>,
    pub radius: Option<f64>,
    pub target_density: Option<f64>,
    pub target_prop: Option<f64>,
    pub urn: String,
    pub school_type: String,
    pub is_state: u32,
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
            region_map.insert(record.pcode.trim().to_owned(), lad.clone());
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
) -> Result<Vec<SchoolInfo<S>>, Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        //.has_headers(true)
        //.flexible(true)
        .from_path(path)?;

    let mut iter = rdr.deserialize::<S>();

    let mut schools: Vec<SchoolInfo<S>> = Vec::new();

    let mut man_n = 0;
    let mut man_a = 0;

    let mut failed = 0;

    for result in iter {
        match result {
            Ok(record) => {
                let ofsted = ofsted_data.get(record.get_urn()).cloned();
                let lad = region_map.get(record.get_pcode()).cloned();
                schools.push(SchoolInfo {
                    record,
                    ofsted,
                    lad,
                })
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

fn main() -> Result<(), Box<dyn Error>> {
    //run_schools(2019..2020)
    run_atomic()
    //combine_csv_files("depr", "depr.csv"); Ok(())
    //assign::circle_test();
}

const STATE_PROP: f64 = 0.8;

fn run_schools(years: std::ops::Range<u32>) -> Result<(), Box<dyn Error>> {
    let regions = load_regions("postcodes.csv")?;
    let ofsted = load_ofsted("ofsted.csv")?;

    let mut geo_map = load_geo_data("geo.csv")?;
    let geonames_data = geo_rust::get_postal_data(Country::UnitedKingdomFull);

    println!("parsed postcodes, {}", regions.len());
    let mut agg_sec: Vec<(u32, Vec<AggregateRecord>)> = Vec::new();

    let mut agg_prim: Vec<(u32, Vec<AggregatePRecord>)> = Vec::new();

    let mut complete_writer_sec = Writer::from_path("all_sec.csv")?;

    let mut complete_writer_prim = Writer::from_path("all_prim.csv")?;

    let to_bng = Proj::new_known_crs("EPSG:4326", "EPSG:27700", None)
        .expect("Failed to create transformation");

    for i in years {
        // let fname: String = format!("scraw_{}.csv", i);
        // sanitize(&fname, &format!("san_{}", &fname));
        //
        // let fname: String = format!("scrawp_{}.csv", i);
        // sanitize(&fname, &format!("san_{}", &fname));
        {
            let fname = format!("san_scraw_{}.csv", i);

            match parse_dset::<String, SchoolRecord>(fname, &ofsted, &regions) {
                Ok(schools) => {
                    let mut ag_schools = Vec::with_capacity(schools.len());
                    for school in schools {
                        let gcseg2 = percentage_string_to_float(&school.record.gcseg2).ok();
                        let gcseg2_dis = percentage_string_to_float(&school.record.gcseg2_dis).ok();

                        let selective = school.record.adm_pol == "SEL";

                        //let loc = geo_data(&school.record.pcode, &mut geo_map, &geonames_data);

                        // Only choose the right kind of schools.
                        let state = TARGET_SCHOOL_TYPES
                            .contains(&school.record.school_type.as_str())
                            && !selective;

                        let pos = if let (Some(lat), Some(long)) = (
                            school.record.lat.parse::<f64>().ok(),
                            school.record.long.parse::<f64>().ok(),
                        ) {
                            to_bng
                                .convert((long, lat))
                                .map(|(x, y)| (x / 1000.0, y / 1000.0)) // Convert to kms
                                .ok()
                        } else {
                            None
                        };

                        let r = AggregateSchoolRecord {
                            year: i,
                            name: school.record.name.clone(),
                            pcode: school.record.pcode.clone(),
                            msoa: school.record.msoa.clone(),
                            target_density: school.record.target_density.parse().ok(),
                            radius: None, // Will allocate once we order by quality.
                            lat: school.record.lat.parse::<f64>().ok(),
                            lng: school.record.long.parse::<f64>().ok(),
                            target_prop: school.record.target_prop.parse::<f64>().ok(),
                            pop: school.record.pop.parse::<f32>().map(|x| x as u32).ok(),
                            x_km: pos.map(|(x, _)| x),
                            y_km: pos.map(|(_, y)| y),
                            urn: school.record.urn.clone(),
                            is_selective: selective as u32,
                            school_type: school.record.school_type.clone(),
                            is_state: state as u32,
                            lad: school.lad,
                            ebacc: school.record.ebacc.clone(),
                            p8: school.record.p8.clone(),
                            of_overall: school.ofsted.as_ref().and_then(|x| x.overall),
                            of_behaviour: school.ofsted.as_ref().and_then(|x| x.behaviour),
                            of_educ: school.ofsted.as_ref().and_then(|x| x.educ),
                            of_pdev: school.ofsted.as_ref().and_then(|x| x.pdev),
                            of_sixthform: school.ofsted.as_ref().and_then(|x| x.sixthform),

                            gcseg2,
                            gcseg2_dis,
                        };

                        //println!("vs: {}, {}, {}, {}, {}, x:{}", r.gcseg2.is_none(), r.x_km.is_none(), r.y_km.is_none(), r.target_density.is_none(), r.pop.is_none(), school.record.pop);

                        ag_schools.push(r);
                    }

                    // Remove schools without the stuff we need to calculate radius.
                    let (drained, mut ag_schools): (Vec<_>, Vec<_>) =
                        ag_schools.into_iter().partition(|r| {
                            (r.gcseg2.is_none()
                                || r.x_km.is_none()
                                || r.y_km.is_none()
                                || r.target_density.is_none()
                                || r.pop.is_none()
                                || r.target_prop.is_none()
                                || r.is_selective == 1
                                || r.is_state == 0)
                        });

                    println!("ag: {}", ag_schools.len());

                    ag_schools
                        .sort_by(|a, b| b.gcseg2.unwrap().partial_cmp(&a.gcseg2.unwrap()).unwrap());

                    // First sort schools by quality. Ordering matches ag_schools one to one.
                    let radials: Vec<assign::RadialArea> = ag_schools
                        .iter()
                        .filter_map(|r| {
                            Some(assign::RadialArea {
                                origin: Vector2::new(r.x_km.unwrap(), r.y_km.unwrap()),
                                area: r.pop.unwrap() as f64
                                    / (r.target_density.unwrap() * r.target_prop.unwrap()),
                            })
                        })
                        .collect();

                    let circles = assign::scale_all(&radials, 0.5, 1e-3, 1000)
                        .ok_or("Failed to scale radials!")?;

                    for (school, circle) in ag_schools.iter_mut().zip(circles.iter()) {
                        school.radius = Some(circle.r);
                    }

                    for school in ag_schools {
                        complete_writer_sec.serialize(&school)?;
                    }

                    for school in drained {
                        complete_writer_sec.serialize(&school)?;
                    }
                    println!("parsed schools {}", i);

                    render::draw_circles_to_png(&circles, 1000, 1000, "map_sec.png");
                }
                Err(e) => println!("Failed to parse school: {}", e),
            }
        }
        // Primary
        {
            let fname = format!("san_scrawp_{}.csv", i);

            match parse_dset::<String, PSchoolRecord>(fname, &ofsted, &regions) {
                Ok(schools) => {
                    let mut ag_schools = Vec::with_capacity(schools.len());
                    for school in schools {
                        let rwm_ta = percentage_string_to_float(&school.record.rwm_ta).ok();
                        let rwm_ta_dis = percentage_string_to_float(&school.record.rwm_ta_dis).ok();
                        let loc = geo_data(&school.record.pcode, &mut geo_map, &geonames_data);

                        let pos = if let (Some(lat), Some(long)) = (
                            school.record.lat.parse::<f64>().ok(),
                            school.record.long.parse::<f64>().ok(),
                        ) {
                            to_bng
                                .convert((long, lat))
                                .map(|(x, y)| (x / 1000.0, y / 1000.0)) // Convert to kms
                                .ok()
                        } else {
                            None
                        };

                        // Only choose the right kind of schools.
                        let state =
                            TARGET_SCHOOL_TYPES.contains(&school.record.school_type.as_str());
                        ag_schools.push(AggregatePSchoolRecord {
                            year: i,
                            name: school.record.name.clone(),
                            pcode: school.record.pcode.clone(),
                            lat: school.record.lat.parse::<f64>().ok(),
                            lng: school.record.long.parse::<f64>().ok(),
                            target_prop: school.record.target_prop.parse::<f64>().ok(),
                            pop: school.record.pop.parse::<f32>().map(|x| x as u32).ok(),
                            x_km: pos.map(|(x, _)| x),
                            y_km: pos.map(|(_, y)| y),
                            target_density: school.record.target_density.parse().ok(),
                            radius: None, // Will allocate once we order by quality.
                            urn: school.record.urn.clone(),
                            is_state: state as u32,
                            school_type: school.record.school_type.clone(),
                            lad: school.lad,
                            of_overall: school.ofsted.as_ref().and_then(|x| x.overall),
                            of_behaviour: school.ofsted.as_ref().and_then(|x| x.behaviour),
                            of_educ: school.ofsted.as_ref().and_then(|x| x.educ),
                            of_pdev: school.ofsted.as_ref().and_then(|x| x.pdev),
                            rwm_ta,
                            rwm_ta_dis,
                        });
                    }

                    // Remove schools without the stuff we need to calculate radius.
                    let (drained, mut ag_schools): (Vec<_>, Vec<_>) =
                        ag_schools.into_iter().partition(|r| {
                            (r.rwm_ta.is_none()
                                || r.x_km.is_none()
                                || r.y_km.is_none()
                                || r.target_density.is_none()
                                || r.pop.is_none()
                                || r.target_prop.is_none()
                                || r.is_state == 0)
                        });

                    println!("ag: {}", ag_schools.len());

                    ag_schools
                        .sort_by(|a, b| b.rwm_ta.unwrap().partial_cmp(&a.rwm_ta.unwrap()).unwrap());

                    // First sort schools by quality. Ordering matches ag_schools one to one.
                    let radials: Vec<assign::RadialArea> = ag_schools
                        .iter()
                        .filter_map(|r| {
                            Some(assign::RadialArea {
                                origin: Vector2::new(r.x_km.unwrap(), r.y_km.unwrap()),
                                area: r.pop.unwrap() as f64
                                    / (r.target_density.unwrap() * r.target_prop.unwrap()),
                            })
                        })
                        .collect();

                    let circles = assign::scale_all(&radials, 0.5, 1e-3, 1000)
                        .ok_or("Failed to scale radials!")?;

                    for (school, circle) in ag_schools.iter_mut().zip(circles.iter()) {
                        school.radius = Some(circle.r);
                    }

                    for school in ag_schools {
                        complete_writer_prim.serialize(&school)?;
                    }

                    for school in drained {
                        complete_writer_prim.serialize(&school)?;
                    }
                    println!("parsed schools {}", i);

                    render::draw_circles_to_png(&circles, 1000, 1000, "map_prim.png");

                    println!("parsed pschools {}", i);
                }
                Err(e) => println!("Failed to parse school: {}", e),
            }
        }
    }

    Ok(())
}

use crate::atomic::{geo_data, load_geo_data};
use csv::ReaderBuilder;
use nalgebra::Vector2;
use proj::Proj;

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
