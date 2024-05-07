use csv::{ReaderBuilder, StringRecord, Writer};
use geo_rust::{get_postcode_location, Country, GeoLocation, PostalData};
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, error::Error, io, os::windows::raw::SOCKET, path::Path, process};

use crate::{load_regions, AggregatePSchoolRecord, AggregateSchoolRecord, Scaler, CUM_RPI_DEFL};


#[derive(serde::Serialize, serde::Deserialize)]
pub struct PcodeRecord {
    id: String,
    year: u32,
    propertytype: String,
    duration: String,
    priceper: Option<f32>,
    price: f32,
    postcode: String,
    tfarea: Option<f32>,
    numberrooms: Option<String>,
    classt: Option<u32>,
    #[serde(rename = "CONSTRUCTION_AGE_BAND")]
    age_band: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ProcessedPcodeRecord {
    pub year: u32,
    pub id: String,
    pub pcode: String,
    pub after_covid: u32,
    pub price: f32,
    pub priceper: Option<f32>,
    pub rpi_defl: Option<f32>,
    pub tfarea: Option<f32>,
    pub numberrooms: Option<u32>,
    pub classt: Option<u32>,
    pub age_band: Option<u32>,
    pub propertytype: String,
    pub lad: String,

    pub dist_manchester: Option<f32>,
    pub dist_liverpool: Option<f32>,

    // The year used to get the edu data.
    pub sec_est_year: Option<u32>,
    pub prim_est_year: Option<u32>,

    // Secondary
    pub closest_sec_urn: Option<String>,
    pub closest_sec_name: Option<String>,
    pub closest_sec_pcode: Option<String>,
    pub closest_sec_dist: Option<f32>,
    pub closest_sec_type: Option<String>,
    pub closest_sec_of_overall: Option<u32>,
    pub closest_sec_of_educ: Option<u32>,
    pub closest_sec_gcseg2: Option<f32>,
    pub closest_sec_gcseg2_dis: Option<f32>,
    pub closest_sec_score: Option<f32>,

    pub weighted_sec_of_overall: Option<f32>,
    pub weighted_sec_of_educ: Option<f32>,
    pub weighted_sec_of_behaviour: Option<f32>,
    pub weighted_sec_of_sixthform: Option<f32>,
    pub weighted_sec_gcseg2: Option<f32>,
    pub weighted_sec_gcseg2_dis: Option<f32>,
    pub weighted_sec_score: Option<f32>,

    // Primary
    pub closest_prim_urn: Option<String>,
    pub closest_prim_name: Option<String>,
    pub closest_prim_pcode: Option<String>,
    pub closest_prim_dist: Option<f32>,
    pub closest_prim_type: Option<String>,
    pub closest_prim_of_overall: Option<u32>,
    pub closest_prim_of_educ: Option<u32>, 
    pub closest_prim_rwm_ta: Option<f32>,
    pub closest_prim_rwm_ta_dis: Option<f32>,
    pub closest_prim_score: Option<f32>,

    pub weighted_prim_of_overall: Option<f32>,
    pub weighted_prim_of_educ: Option<f32>, 
    pub weighted_prim_of_behaviour: Option<f32>, 
    pub weighted_prim_rwm_ta: Option<f32>,
    pub weighted_prim_rwm_ta_dis: Option<f32>,
    pub weighted_prim_score: Option<f32>,
    

   
    

     // Weighted by 1/square of distance from school

    

}

#[derive(Serialize, Deserialize)]
pub struct GeoRecord {
    pcode: String,
    lat: f64,
    long: f64,
}

pub fn parse_postcodes<P: AsRef<Path>>(path: P, region_map: &HashMap<String, String>) -> Result<HashMap<String, Vec<(PcodeRecord, String)>>, Box<dyn Error>> {
    let mut pcodes: HashMap<String, Vec<(PcodeRecord, String)>> = HashMap::new();

    // let mut rdr = ReaderBuilder::new()
    //     //.has_headers(true)
    //     //.flexible(true)
    //     .from_path(&path)?;

    // for result in rdr.into_records() {
        
    //     match result {
    //         Ok(record) => {
    //             println!("a{:?}", record);
    //         }
    //         Err(e) => {
    //             println!("x{}", e);
    //         }
    //     }
    // }

    let mut rdr = ReaderBuilder::new()
    //.has_headers(true)
    //.flexible(true)
    .from_path(path)?;
    let mut iter = rdr.deserialize::<PcodeRecord>();

    for result in iter {
        match result {
            Ok(record) => {
                if let Some(lad) = region_map.get(&record.postcode) {
                    if let Some(v) = pcodes.get_mut(&record.postcode) {
                        v.push((record, lad.clone()));
                    } else {
                        pcodes.insert(record.postcode.clone(), vec![(record, lad.clone())]);
                    }
                }
            }
            Err(e) => {
                println!("{}", e);
            }
        }
    }

    Ok(pcodes)
}

pub fn load_school_data<P: AsRef<Path>, S: DeserializeOwned>(path: P) -> Result<Vec<S>, Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
    //.has_headers(true)
    //.flexible(true)
    .from_path(path)?;
    let mut iter = rdr.deserialize::<S>();

    Ok(iter.filter_map(|x| x.ok()).collect())
}

pub fn load_geo_data<P: AsRef<Path>>(path: P) -> Result<HashMap<String, GeoRecord>, Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
    //.has_headers(true)
    //.flexible(true)
    .from_path(path)?;
    let mut iter = rdr.deserialize::<GeoRecord>();

    let mut map = HashMap::new();

    for record in iter {
        if let Ok(record) = record {
            map.insert(record.pcode.clone(), record);
        }
    }

    Ok(map)
}

fn geo_data(pcode: &str, map: &mut HashMap<String, GeoRecord>, geonames_data: &[PostalData]) -> Option<GeoLocation> {
    
    if let Some(v) = map.get(pcode.trim()) {
        Some(GeoLocation { latitude: v.lat, longitude: v.long })
    } else {
        if let Some(d) = get_postcode_location(pcode.trim(), geonames_data) {
            let pcode = pcode.trim().to_owned();
            map.insert(pcode.trim().to_owned(), GeoRecord { pcode, lat: d.latitude, long: d.longitude });
            Some(d)
        } else {
            None
        }
    }
}

const MAX_DIST: f32 = 5.0;

pub fn aggregate_pdata(pcodes: HashMap<String, Vec<(PcodeRecord, String)>>, sec_schools: Vec<AggregateSchoolRecord>, prim_schools: Vec<AggregatePSchoolRecord>, geo_map: &mut HashMap<String, GeoRecord>, year_range: std::ops::Range<u32>) -> Vec<ProcessedPcodeRecord> {
    let geonames_data = geo_rust::get_postal_data(Country::UnitedKingdomFull);

    let manchester_geo = geo_data("M1 1AF", geo_map, &geonames_data).unwrap();
    let liverpool_geo = geo_data("L1 0AA", geo_map, &geonames_data).unwrap();
    
    let mut processed_records: Vec<ProcessedPcodeRecord> = Vec::new();
    let len = pcodes.len();

    let mut sec_map: HashMap<u32, Vec<(Option<GeoLocation>, AggregateSchoolRecord)>> = HashMap::new();
    println!("Loading sec school geo data - may take some time...");

    for sch in sec_schools {
        let loc = geo_data(&sch.pcode, geo_map, &geonames_data);

        if let Some(m) = sec_map.get_mut(&sch.year) {
            m.push((loc, sch));
        } else {
            sec_map.insert(sch.year, vec![(loc, sch)]);
        }
    }

    let mut prim_map: HashMap<u32, Vec<(Option<GeoLocation>, AggregatePSchoolRecord)>> = HashMap::new();
    println!("Loading prim school geo data - may take some time...");

    for sch in prim_schools {
        let loc =  geo_data(&sch.pcode, geo_map, &geonames_data);

        if let Some(m) = prim_map.get_mut(&sch.year) {
            m.push((loc, sch));
        } else {
            prim_map.insert(sch.year, vec![(loc, sch)]);
        }
    }


    for (i, (pcode, records)) in pcodes.into_iter().enumerate() {
        if i % 1000 == 0 {
            println!("Parsing {} of {} pcodes ({} records)", i, len, records.len());
        }
        let pc_loc =  geo_data(&pcode, geo_map, &geonames_data);
        for (j, (record, lad)) in records.into_iter().enumerate() {

            if (year_range.contains(&record.year)) {

                let mut closest_sec_dist: Option<f32> = None;
                let mut closest_prim_dist: Option<f32> = None;
            
                let mut closest_sec: Option<AggregateSchoolRecord> = None;
                let mut closest_prim: Option<AggregatePSchoolRecord> = None;
    
                let mut weighted_sec_score: Scaler = Scaler::new();
                let mut weighted_sec_of_educ: Scaler = Scaler::new();
                let mut weighted_sec_of_behaviour: Scaler = Scaler::new();
                let mut weighted_sec_gcseg2: Scaler = Scaler::new();
                let mut weighted_sec_gcseg2_dis: Scaler = Scaler::new();
                let mut weighted_sec_of_overall: Scaler = Scaler::new();
                let mut weighted_sec_of_sixthform: Scaler = Scaler::new();
                
                let mut weighted_prim_score: Scaler = Scaler::new();
                let mut weighted_prim_of_educ: Scaler = Scaler::new();
                let mut weighted_prim_of_behaviour: Scaler = Scaler::new();
                let mut weighted_prim_rwm_ta: Scaler = Scaler::new();
                let mut weighted_prim_rwm_ta_dis: Scaler = Scaler::new();
                let mut weighted_prim_of_overall: Scaler = Scaler::new();

                let mut sec_est_year: Option<u32> = None;
                let mut prim_est_year: Option<u32> = None;

                let mut dist_manchester: Option<f32> = None;
                let mut dist_liverpool: Option<f32> = None;

                if let Some(loc) = &pc_loc {
                    dist_manchester = Some(loc.distance(&manchester_geo) as f32);
                    dist_liverpool = Some(loc.distance(&liverpool_geo) as f32);

                    let mut sec_list: Option<&Vec<(Option<GeoLocation>, AggregateSchoolRecord)>> = None;
                    if let Some(x) = sec_map.get(&record.year) {
                        sec_est_year = Some(record.year);
                        sec_list = Some(x);
                    } else {
                        let mut y = record.year - 1;
                        while year_range.contains(&y) {
                            if let Some(x) = sec_map.get(&y) {
                                sec_est_year = Some(y);
                                sec_list = Some(x);
                                break;
                            } 
                            y -= 1;
                        }
                    }
                    if let Some(sec_list) = sec_list {
                        for (i, (school_loc, school)) in sec_list.iter().enumerate() {
                            if school.is_state != 1 || school.is_selective == 1 {
                                continue;
                            }
                            if let Some(school_loc) = school_loc {
                                let dist = loc.distance(&school_loc) as f32;
                                if closest_sec_dist.map(|x| dist < x).unwrap_or(true) {
                                    // Update
                                    closest_sec_dist = Some(dist);
                                    closest_sec = Some(school.clone());
                                }
        
                                let w = if dist >= MAX_DIST { 0.0 } else { (MAX_DIST - dist) / MAX_DIST };

                                // Add weights.
                                if w > 0.0 {
                                    if let Some(x) = school.score {
                                        weighted_sec_score.add(x, w);
                                    }
            
                                    if let Some(x) = school.of_educ {
                                        weighted_sec_of_educ.add(x as f32, w);
                                    }
                                    
                                    if let Some(x) = school.of_behaviour {
                                        weighted_sec_of_behaviour.add(x as f32, w);
                                    }
    
                                    if let Some(x) = school.gcseg2 {
                                        weighted_sec_gcseg2.add(x as f32, w);
                                    }
    
                                    if let Some(x) = school.gcseg2_dis {
                                        weighted_sec_gcseg2_dis.add(x as f32, w);
                                    }

                                    if let Some(x) = school.of_overall {
                                        weighted_sec_of_overall.add(x as f32, w);
                                    }
                                    if let Some(x) = school.of_sixthform {
                                        weighted_sec_of_sixthform.add(x as f32, w);
                                    }
                                }

                            }
                        }
                    }
                    
                    let mut prim_list: Option<&Vec<(Option<GeoLocation>, AggregatePSchoolRecord)>> = None;
                    if let Some(x) = prim_map.get(&record.year) {
                        prim_est_year = Some(record.year);
                        prim_list = Some(x);
                    } else {
                        let mut y: u32 = record.year - 1;
                        while year_range.contains(&y) {
                            if let Some(x) = prim_map.get(&y) {
                                prim_est_year = Some(y);
                                prim_list = Some(x);
                                break;
                            } 
                            y -= 1;
                        }
                    }
    
                    if let Some(prim_list) = prim_list {
                        for (school_loc, school) in prim_list.iter() {
                            if school.is_state != 1 {
                                continue;
                            }
                            if let Some(school_loc) = school_loc {
                                let dist = loc.distance(&school_loc) as f32;
                                if closest_prim_dist.map(|x| dist < x).unwrap_or(true) {
                                    // Update
                                    closest_prim_dist = Some(dist);
                                    closest_prim = Some(school.clone());
                                }
        

                                let w = if dist >= MAX_DIST { 0.0 } else { (MAX_DIST - dist) / MAX_DIST };
                                
                                // Add weights.
                                if w > 0.0 {
                                    if let Some(x) = school.score {
                                        weighted_prim_score.add(x, w);
                                    }
            
                                    if let Some(x) = school.of_educ {
                                        weighted_prim_of_educ.add(x as f32, w);
                                    }
                                    
                                    if let Some(x) = school.of_behaviour {
                                        weighted_prim_of_behaviour.add(x as f32, w);
                                    }

                                    if let Some(x) = school.rwm_ta {
                                        weighted_prim_rwm_ta.add(x as f32, w);
                                    }

                                    if let Some(x) = school.rwm_ta_dis {
                                        weighted_prim_rwm_ta_dis.add(x as f32, w);
                                    }

                                    if let Some(x) = school.of_overall {
                                        weighted_prim_of_overall.add(x as f32, w);
                                    }
                                }
                            }
                        }
                    }
    
                } else {
                    println!("No postcode location for: {}", &pcode);
                }
    
                let age_band = match record.age_band.as_ref().map(|x| x.trim()) {
                    Some("England and Wales: 1900-1929") => Some(1900),
                    Some("England and Wales: 2007 onwards") => Some(2007),
                    Some("England and Wales: 2003-2006") => Some(2003),
                    Some("England and Wales: 1996-2002") => Some(1996),
                    Some("England and Wales: 1930-1949") => Some(1930),
                    Some("England and Wales: 1983-1990") => Some(1983),
                    Some("England and Wales: 1967-1975") => Some(1967),
                    Some("England and Wales: 1976-1982") => Some(1976),
                    Some("England and Wales: 1991-1995") => Some(1991),
                    Some("England and Wales: before 1900") => Some(1880),
                    Some("England and Wales: 1950-1966") => Some(1950),
                    _ => None,
                };

                let rpi_defl = CUM_RPI_DEFL.get((record.year - 2017) as usize).copied();
    
                processed_records.push(ProcessedPcodeRecord {
                    id: record.id,
                    after_covid: (record.year >= 2021) as u32,
                    age_band: age_band,
                    classt: record.classt,
                    price: record.price,
                    numberrooms: record.numberrooms.and_then(|x| x.parse::<u32>().ok()),
                    tfarea: record.tfarea.and_then(|x| if x.is_normal() { Some(x) } else { None }),
                    priceper: record.priceper,
                    year: record.year,
                    rpi_defl,
                    propertytype: record.propertytype,
                    lad,
                    pcode: record.postcode,
    
                    sec_est_year,
                    prim_est_year,

                    dist_manchester,
                    dist_liverpool,

                    closest_prim_dist,
                    closest_prim_urn: closest_prim.as_ref().map(|x| x.urn.clone()),
                    closest_prim_name: closest_prim.as_ref().map(|x| x.name.clone()),
                    closest_prim_type: closest_prim.as_ref().map(|x| x.school_type.clone()),
                    closest_prim_of_educ:  closest_prim.as_ref().and_then(|x| x.of_educ), 
                    closest_prim_pcode: closest_prim.as_ref().map(|x| x.pcode.clone()), 
                    closest_prim_score: closest_prim.as_ref().and_then(|x| x.score),
                    closest_prim_rwm_ta: closest_prim.as_ref().and_then(|x| x.rwm_ta),
                    closest_prim_rwm_ta_dis: closest_prim.as_ref().and_then(|x| x.rwm_ta_dis),
                    closest_prim_of_overall: closest_prim.as_ref().and_then(|x| x.of_overall),
                    weighted_prim_of_educ: weighted_prim_of_educ.ave(),
                    weighted_prim_score: weighted_prim_score.ave(),
                    weighted_prim_rwm_ta: weighted_prim_rwm_ta.ave(),
                    weighted_prim_rwm_ta_dis: weighted_prim_rwm_ta_dis.ave(),
                    weighted_prim_of_behaviour: weighted_prim_of_behaviour.ave(),
                    weighted_prim_of_overall: weighted_prim_of_overall.ave(),

                    closest_sec_dist,
                    closest_sec_urn: closest_sec.as_ref().map(|x| x.urn.clone()),
                    closest_sec_type: closest_sec.as_ref().map(|x| x.school_type.clone()),
                    closest_sec_name: closest_sec.as_ref().map(|x| x.name.clone()),
                    closest_sec_of_educ: closest_sec.as_ref().and_then(|x| x.of_educ),
                    closest_sec_pcode: closest_sec.as_ref().map(|x| x.pcode.clone()),
                    closest_sec_score: closest_sec.as_ref().and_then(|x| x.score),
                    closest_sec_gcseg2: closest_sec.as_ref().and_then(|x| x.gcseg2),
                    closest_sec_gcseg2_dis: closest_sec.as_ref().and_then(|x| x.gcseg2_dis),
                    closest_sec_of_overall: closest_sec.as_ref().and_then(|x| x.of_overall),
                    weighted_sec_gcseg2: weighted_sec_gcseg2.ave(),
                    weighted_sec_gcseg2_dis: weighted_sec_gcseg2_dis.ave(),
                    weighted_sec_of_educ: weighted_sec_of_educ.ave(),
                    weighted_sec_score: weighted_sec_score.ave(),
                    weighted_sec_of_behaviour: weighted_sec_of_behaviour.ave(),
                    weighted_sec_of_overall: weighted_sec_of_overall.ave(),
                    weighted_sec_of_sixthform: weighted_sec_of_sixthform.ave(),
                });
            }
        }
    }

    processed_records
}

pub fn run_atomic() -> Result<(), Box<dyn Error>> {
    let regions = load_regions("postcodes.csv")?;

    let postcodes = parse_postcodes("pdata.csv", &regions)?;
    println!("Parsed {} postcodes", postcodes.len());

    let sec_data = load_school_data("scout_full_sec.csv")?;
    println!("Loaded {} sec schools", sec_data.len());

    let prim_data = load_school_data("scout_full_prim.csv")?;
    println!("Loaded {} prim schools", prim_data.len());

    let mut geo_data = load_geo_data("geo.csv").unwrap_or(HashMap::new());

    let records = aggregate_pdata(postcodes, sec_data, prim_data, &mut geo_data, 2017..2024);
    println!("Aggregated {} pcodes", records.len());
    {
        let mut writer = Writer::from_path("atomic.csv")?;
        for record in records {
            writer.serialize(record);
        }
    }
    

    {
        // Write geo data
        let mut writer = Writer::from_path("geo.csv")?;
        let mut pcodelist = Writer::from_path("pcodelist.csv")?;
        for (k, v) in geo_data {
            let mut prec = StringRecord::new();
            prec.push_field(&v.pcode);
            pcodelist.write_record(&prec);

            writer.serialize(v);
        }
    }


    Ok(())
}