use csv::{ReaderBuilder, StringRecord, Writer};
use geo_rust::{get_postcode_location, Country, GeoLocation, PostalData};
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{clone, collections::HashMap, error::Error, fs::File, io::{self, Write}, os::windows::raw::SOCKET, path::Path, process, sync::{Arc, Mutex}};

use crate::{first_letters, load_regions, AggregatePSchoolRecord, AggregateSchoolRecord, Scaler, CUM_RPI_DEFL};


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
    pub lad: Option<String>,
    
    pub lat: Option<f64>,
    pub lng: Option<f64>,

    pub nearest_town_name: Option<String>,
    pub nearest_town_dist: Option<f64>,
    pub nearest_admin_name: Option<String>,
    pub nearest_town_popn: Option<u32>,

    pub nearest_city_name: Option<String>,
    pub nearest_city_dist: Option<f64>,
    pub nearest_city_popn: Option<u32>,

    pub dist_london: Option<f64>,

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

    pub weighted_sec_of_overall: Option<f32>,
    pub weighted_sec_of_educ: Option<f32>,
    pub weighted_sec_of_behaviour: Option<f32>,
    pub weighted_sec_of_sixthform: Option<f32>,
    pub weighted_sec_gcseg2: Option<f32>,
    pub weighted_sec_gcseg2_dis: Option<f32>,

    pub best_sec_gcseg2: Option<f32>,
    pub best_sec_gcseg2_dis: Option<f32>,
    pub best_sec_of_overall: Option<u32>,

    pub v2_sec: Option<f32>,
    pub v2_sec_dis: Option<f32>,

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

    pub weighted_prim_of_overall: Option<f32>,
    pub weighted_prim_of_educ: Option<f32>, 
    pub weighted_prim_of_behaviour: Option<f32>, 
    pub weighted_prim_rwm_ta: Option<f32>,
    pub weighted_prim_rwm_ta_dis: Option<f32>,

    pub best_prim_rwm_ta: Option<f32>,
    pub best_prim_rwm_ta_dis: Option<f32>,
    pub best_prim_of_overall: Option<u32>,

    pub v2_prim: Option<f32>,
    pub v2_prim_dis: Option<f32>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RegionalProcessedPcodeRecord {
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
    pub lad: Option<String>,
    pub region: Option<String>,
    pub pcode_area: Option<String>,
    pub lat: Option<f64>,
    pub lng: Option<f64>,

    
    pub nearest_town_name: Option<String>,
    pub nearest_town_dist: Option<f64>,
    pub nearest_admin_name: Option<String>,
    pub nearest_town_popn: Option<u32>,

    pub nearest_city_name: Option<String>,
    pub nearest_city_dist: Option<f64>,
    pub nearest_city_popn: Option<u32>,

    pub dist_london: Option<f64>,

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

    pub weighted_sec_of_overall: Option<f32>,
    pub weighted_sec_of_educ: Option<f32>,
    pub weighted_sec_of_behaviour: Option<f32>,
    pub weighted_sec_of_sixthform: Option<f32>,
    pub weighted_sec_gcseg2: Option<f32>,
    pub weighted_sec_gcseg2_dis: Option<f32>,

    pub best_sec_gcseg2: Option<f32>, // selected by school with highest best_sec_gcseg2_dis
    pub best_sec_gcseg2_dis: Option<f32>,
    pub best_sec_of_overall: Option<u32>,

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

    pub weighted_prim_of_overall: Option<f32>,
    pub weighted_prim_of_educ: Option<f32>, 
    pub weighted_prim_of_behaviour: Option<f32>, 
    pub weighted_prim_rwm_ta: Option<f32>,
    pub weighted_prim_rwm_ta_dis: Option<f32>,

    pub best_prim_rwm_ta: Option<f32>, // selected by school with highest best_prim_rwm_ta_dis
    pub best_prim_rwm_ta_dis: Option<f32>,
    pub best_prim_of_overall: Option<u32>,
}

impl RegionalProcessedPcodeRecord {
    pub fn new(record: ProcessedPcodeRecord, region: Option<String>, pcode_area: Option<String>) -> Self {
        Self {
            region,
            pcode_area,
            lat: record.lat,
            lng: record.lng,
            year: record.year,
            id: record.id,
            pcode: record.pcode,
            after_covid: record.after_covid,
            price: record.price,
            priceper: record.priceper,
            rpi_defl: record.rpi_defl,
            tfarea: record.tfarea,
            numberrooms: record.numberrooms,
            classt: record.classt,
            age_band: record.age_band,
            propertytype: record.propertytype,
            lad: record.lad,
            nearest_town_dist: record.nearest_town_dist,
            nearest_town_name: record.nearest_town_name,
            nearest_admin_name: record.nearest_admin_name,
            nearest_town_popn: record.nearest_town_popn,
            nearest_city_name: record.nearest_city_name,
            nearest_city_dist: record.nearest_city_dist,
            nearest_city_popn: record.nearest_city_popn,
            dist_london: record.dist_london,
            sec_est_year: record.sec_est_year,
            prim_est_year: record.prim_est_year,
            closest_sec_urn: record.closest_sec_urn,
            closest_sec_name: record.closest_sec_name,
            closest_sec_pcode: record.closest_sec_pcode,
            closest_sec_dist: record.closest_sec_dist,
            closest_sec_type: record.closest_sec_type,
            closest_sec_of_overall: record.closest_sec_of_overall,
            closest_sec_of_educ: record.closest_sec_of_educ,
            closest_sec_gcseg2: record.closest_sec_gcseg2,
            closest_sec_gcseg2_dis: record.closest_sec_gcseg2_dis,
            weighted_sec_of_overall: record.weighted_sec_of_overall,
            weighted_sec_of_educ: record.weighted_sec_of_educ,
            weighted_sec_of_behaviour: record.weighted_sec_of_behaviour,
            weighted_sec_of_sixthform: record.weighted_sec_of_sixthform,
            weighted_sec_gcseg2: record.weighted_sec_gcseg2,
            weighted_sec_gcseg2_dis: record.weighted_sec_gcseg2_dis,
            best_sec_gcseg2: record.best_sec_gcseg2,
            best_sec_gcseg2_dis: record.best_sec_gcseg2_dis,
            best_sec_of_overall: record.best_sec_of_overall,
            closest_prim_urn: record.closest_prim_urn,
            closest_prim_name: record.closest_prim_name,
            closest_prim_pcode: record.closest_prim_pcode,
            closest_prim_dist: record.closest_prim_dist,
            closest_prim_type: record.closest_prim_type,
            closest_prim_of_overall: record.closest_prim_of_overall,
            closest_prim_of_educ: record.closest_prim_of_educ,
            closest_prim_rwm_ta: record.closest_prim_rwm_ta,
            closest_prim_rwm_ta_dis: record.closest_prim_rwm_ta_dis,
            weighted_prim_of_overall: record.weighted_prim_of_overall,
            weighted_prim_of_educ: record.weighted_prim_of_educ,
            weighted_prim_of_behaviour: record.weighted_prim_of_behaviour,
            weighted_prim_rwm_ta: record.weighted_prim_rwm_ta,
            weighted_prim_rwm_ta_dis: record.weighted_prim_rwm_ta_dis,
            best_prim_rwm_ta: record.best_prim_rwm_ta,
            best_prim_rwm_ta_dis: record.best_prim_rwm_ta_dis,
            best_prim_of_overall: record.best_prim_of_overall,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TownRecord {
    city: String,
    lat: f64,
    lng: f64,
    admin_name: String,
    population: u32,
    population_proper: u32,
}

#[derive(Clone)]
pub struct Town {
    record: TownRecord,
    loc: GeoLocation,
}

#[derive(Serialize, Deserialize)]
pub struct GeoRecord {
    pcode: String,
    lat: f64,
    long: f64,
}

#[derive(Serialize, Deserialize)]
pub struct RegionRecord {
    pcode_area: String,
    area_name: String,
    region: String,
}

pub fn parse_cities<P: AsRef<Path>>(path: P) -> Result<Vec<Town>, Box<dyn Error>> {

    let mut cities = Vec::new();

    let mut rdr = ReaderBuilder::new()
    //.has_headers(true)
    //.flexible(true)
    .from_path(path)?;
    let mut iter = rdr.deserialize::<TownRecord>();
    
    for result in iter {
        match result {
            Ok(record) => {
                let loc = GeoLocation { latitude: record.lat, longitude: record.lng };
                cities.push(Town{ loc, record });
            }
            Err(e) => {
                println!("{}", e);
            }
        }
    }

    Ok(cities)
}  

pub fn parse_postcodes<P: AsRef<Path>>(path: P, region_map: &HashMap<String, String>, year_range: std::ops::Range<u32>) -> Result<HashMap<String, Vec<(PcodeRecord, Option<String>)>>, Box<dyn Error>> {
    let mut pcodes: HashMap<String, Vec<(PcodeRecord, Option<String>)>> = HashMap::new();

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
                if year_range.contains(&record.year) {
                    if valid_region(&record.postcode) {
                        let lad = region_map.get(&record.postcode);
                        if let Some(v) = pcodes.get_mut(&record.postcode) {
                            v.push((record, lad.cloned()));
                        } else {
                            pcodes.insert(record.postcode.clone(), vec![(record, lad.cloned())]);
                        }
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

pub struct GeoData<W: Write> {
    writer: Writer<W>,
    map: HashMap<String, GeoRecord>,
}

pub struct CGeoData {
    map: HashMap<String, GeoRecord>
}

// pub fn load_geo_data<P: AsRef<Path>>(path: P) -> Result<GeoData<File>, Box<dyn Error>> {
    
//     let map = {
//         let mut map = HashMap::new();
//         if let Ok(mut rdr) = ReaderBuilder::new()
//         //.has_headers(true)
//         //.flexible(true)
//         .from_path(&path) {
//             let mut iter = rdr.deserialize::<GeoRecord>();
//             for record in iter {
//                 if let Ok(record) = record {
//                     map.insert(record.pcode.clone(), record);
//                 }
//             }
//         }
//         map
//     };

//     let writer = Writer::from_path(path)?;

//     Ok(GeoData { writer, map })
// }

pub fn load_geo_data<P: AsRef<Path>>(path: P) -> Result<CGeoData, Box<dyn Error>> {
    
    let map = {
        let mut map = HashMap::new();
        if let Ok(mut rdr) = ReaderBuilder::new()
        //.has_headers(true)
        //.flexible(true)
        .from_path(&path) {
            let mut iter = rdr.deserialize::<GeoRecord>();
            for record in iter {
                if let Ok(record) = record {
                    map.insert(record.pcode.clone(), record);
                }
            }
        }
        map
    };

    Ok(CGeoData { map })
}

pub fn load_regional_data<P: AsRef<Path>>(path: P) -> Result<HashMap<String, RegionRecord>, Box<dyn Error>> {
    let mut map = HashMap::new();
    if let Ok(mut rdr) = ReaderBuilder::new()
    //.has_headers(true)
    //.flexible(true)
    .from_path(path) {
        let mut iter = rdr.deserialize::<RegionRecord>();
        for record in iter {
            if let Ok(record) = record {
                map.insert(record.pcode_area.clone(), record);
            }
        }
    }
    Ok(map)
}

// pub fn geo_data<W: Write>(pcode: &str, map: &mut GeoData<W>, geonames_data: &[PostalData]) -> Option<GeoLocation> {
//     if let Some(v) = map.map.get(pcode.trim()) {
//         Some(GeoLocation { latitude: v.lat, longitude: v.long })
//     } else {
//         if let Some(d) = get_postcode_location(pcode.trim(), geonames_data) {
//             let pcode = pcode.trim().to_owned();
//             let rec = GeoRecord { pcode: pcode.clone(), lat: d.latitude, long: d.longitude };
//             map.writer.serialize(&rec);
//             map.map.insert(pcode.trim().to_owned(), rec);
//             Some(d)
//         } else {
//             None
//         }
//     }
// }

pub fn geo_data(pcode: &str, map: &CGeoData, geonames_data: &[PostalData]) -> Option<GeoLocation> {
    if let Some(v) = map.map.get(pcode.trim()) {
        Some(GeoLocation { latitude: v.lat, longitude: v.long })
    } else {
        if let Some(d) = get_postcode_location(pcode.trim(), geonames_data) {
            Some(d)
        } else {
            None
        }
    }
}

const MAX_DIST: f32 = 5.0;
const LONDON: GeoLocation = GeoLocation { latitude: 51.5072, longitude: -0.1275 };

pub fn aggregate_pdata(writer: Arc<Mutex<Writer<File>>>, pcodes: HashMap<String, Vec<(PcodeRecord, Option<String>)>>, sec_map:Arc<HashMap<u32, Vec<AggregateSchoolRecord>>>, prim_map: Arc<HashMap<u32, Vec<AggregatePSchoolRecord>>>, towns: Arc<Vec<Town>>, cities: Arc<Vec<Town>>, geo_map: Arc<CGeoData>, regional_data: Arc<HashMap<String, RegionRecord>>, year_range: std::ops::Range<u32>) -> Result<(), Box<dyn Error>> {
    let geonames_data = geo_rust::get_postal_data(Country::UnitedKingdomFull);
    
    //let mut writer = Writer::from_path(path)?;

    //let mut processed_records: Vec<ProcessedPcodeRecord> = Vec::new();
    let len = pcodes.len();


    for (i, (pcode, records)) in pcodes.into_iter().enumerate() {
        if i % 1000 == 0 {
            println!("Parsing {} of {} pcodes ({} records)", i, len, records.len());
        }
        let pc_loc =  geo_data(&pcode, &geo_map, &geonames_data);

        let mut closest_town: Option<Town> = None;
        let mut closest_town_dist: Option<f64> = None;

        let mut closest_city: Option<Town> = None;
        let mut closest_city_dist: Option<f64> = None;

        let mut dist_london: Option<f64> = None;
        
        let mut lat = None;
        let mut lng = None;

        if let Some(loc) = &pc_loc {
            lat = Some(loc.latitude);
            lng = Some(loc.longitude);

            dist_london = Some(loc.distance(&LONDON));
            // Find closest
            for town in towns.iter() {
                let dist = loc.distance(&town.loc);
                if closest_town_dist.map(|x| dist < x).unwrap_or(true) {
                    // Update
                    closest_town_dist = Some(dist);
                    closest_town = Some(town.clone());
                }
            }

            for city in cities.iter() {
                let dist = loc.distance(&city.loc);
                if closest_city_dist.map(|x| dist < x).unwrap_or(true) {
                    // Update
                    closest_city_dist = Some(dist);
                    closest_city = Some(city.clone());
                }
            }

            let (region, pcode_area) = if let Some(area_code) = first_letters(&pcode) {
                regional_data.get(&area_code).map_or((None, None), |x| (Some(x.region.clone()), Some(x.area_name.clone())))
            } else {
                (None, None)
            };
        
            for (j, (record, lad)) in records.into_iter().enumerate() {
                let mut closest_sec_dist: Option<f32> = None;
                let mut closest_prim_dist: Option<f32> = None;
            
                let mut closest_sec: Option<AggregateSchoolRecord> = None;
                let mut closest_prim: Option<AggregatePSchoolRecord> = None;
        
                let mut weighted_sec_of_educ: Scaler = Scaler::new();
                let mut weighted_sec_of_behaviour: Scaler = Scaler::new();
                let mut weighted_sec_gcseg2: Scaler = Scaler::new();
                let mut weighted_sec_gcseg2_dis: Scaler = Scaler::new();
                let mut weighted_sec_of_overall: Scaler = Scaler::new();
                let mut weighted_sec_of_sixthform: Scaler = Scaler::new();
                
                let mut weighted_prim_of_educ: Scaler = Scaler::new();
                let mut weighted_prim_of_behaviour: Scaler = Scaler::new();
                let mut weighted_prim_rwm_ta: Scaler = Scaler::new();
                let mut weighted_prim_rwm_ta_dis: Scaler = Scaler::new();
                let mut weighted_prim_of_overall: Scaler = Scaler::new();

                let mut best_sec_gcseg2: Option<f32> = None;
                let mut best_sec_gcseg2_dis: Option<f32> = None;
                let mut best_sec_of_overall: Option<u32> = None; // Separate to above

                let mut best_prim_rwm_ta: Option<f32> = None;
                let mut best_prim_rwm_ta_dis: Option<f32> = None;
                let mut best_prim_of_overall: Option<u32> = None; // Separate to above
        
        
                let mut sec_est_year: Option<u32> = None;
                let mut prim_est_year: Option<u32> = None;

                let rpi_defl = CUM_RPI_DEFL.get((record.year - 2017) as usize).copied();
                let mut sec_list: Option<&Vec<AggregateSchoolRecord>> = None;
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
                    for (i, school) in sec_list.iter().enumerate() {
                        if school.is_state != 1 || school.is_selective == 1 {
                            continue;
                        }
                        if let Some(school_loc) = school.location() {
                            let dist = loc.distance(&school_loc) as f32;
                            if closest_sec_dist.map(|x| dist < x).unwrap_or(true) {
                                // Update
                                closest_sec_dist = Some(dist);
                                closest_sec = Some(school.clone());
                            }

                            let w = if dist >= MAX_DIST { 0.0 } else { (MAX_DIST - dist) / MAX_DIST };

                            // Add weights.
                            if w > 0.0 {
                                if best_sec_gcseg2.map(|x| school.gcseg2 > Some(x)).unwrap_or(true) {
                                    best_sec_gcseg2_dis = school.gcseg2_dis;
                                    best_sec_gcseg2 = school.gcseg2;
                                }

                                if best_sec_of_overall.map(|x| school.of_overall < Some(x)).unwrap_or(true) {
                                    best_sec_of_overall = school.of_overall;
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
                
                let mut prim_list: Option<&Vec< AggregatePSchoolRecord>> = None;
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
                    for school in prim_list.iter() {
                        if school.is_state != 1 {
                            continue;
                        }
                        if let Some(school_loc) = school.location() {
                            let dist = loc.distance(&school_loc) as f32;
                            if closest_prim_dist.map(|x| dist < x).unwrap_or(true) {
                                // Update
                                closest_prim_dist = Some(dist);
                                closest_prim = Some(school.clone());
                            }


                            let w = if dist >= MAX_DIST { 0.0 } else { (MAX_DIST - dist) / MAX_DIST };
                            
                            // Add weights.
                            if w > 0.0 {
                                if best_prim_rwm_ta.map(|x| school.rwm_ta > Some(x)).unwrap_or(true) {
                                    best_prim_rwm_ta_dis = school.rwm_ta_dis;
                                    best_prim_rwm_ta = school.rwm_ta;
                                }

                                if best_prim_of_overall.map(|x| school.of_overall < Some(x)).unwrap_or(true) {
                                    best_prim_of_overall = school.of_overall;
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

                if (year_range.contains(&record.year)) {

                    writer.lock().unwrap().serialize(&RegionalProcessedPcodeRecord {
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
                        lat,
                        lng,
                        region: region.clone(),
                        pcode_area: pcode_area.clone(),
                        sec_est_year,
                        prim_est_year,

                        dist_london,
                        nearest_town_dist: closest_town_dist,
                        nearest_admin_name: closest_town.as_ref().map(|x| x.record.admin_name.clone()),
                        nearest_town_popn: closest_town.as_ref().map(|x| x.record.population_proper),
                        nearest_town_name: closest_town.as_ref().map(|x| x.record.city.clone()),

                        nearest_city_name: closest_city.as_ref().map(|x| x.record.city.clone()),
                        nearest_city_popn: closest_city.as_ref().map(|x| x.record.population_proper),
                        nearest_city_dist: closest_city_dist,

                        closest_prim_dist,
                        closest_prim_urn: closest_prim.as_ref().map(|x| x.urn.clone()),
                        closest_prim_name: closest_prim.as_ref().map(|x| x.name.clone()),
                        closest_prim_type: closest_prim.as_ref().map(|x| x.school_type.clone()),
                        closest_prim_of_educ:  closest_prim.as_ref().and_then(|x| x.of_educ), 
                        closest_prim_pcode: closest_prim.as_ref().map(|x| x.pcode.clone()), 
                        closest_prim_rwm_ta: closest_prim.as_ref().and_then(|x| x.rwm_ta),
                        closest_prim_rwm_ta_dis: closest_prim.as_ref().and_then(|x| x.rwm_ta_dis),
                        closest_prim_of_overall: closest_prim.as_ref().and_then(|x| x.of_overall),
                        weighted_prim_of_educ: weighted_prim_of_educ.ave(),
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
                        closest_sec_gcseg2: closest_sec.as_ref().and_then(|x| x.gcseg2),
                        closest_sec_gcseg2_dis: closest_sec.as_ref().and_then(|x| x.gcseg2_dis),
                        closest_sec_of_overall: closest_sec.as_ref().and_then(|x| x.of_overall),
                        weighted_sec_gcseg2: weighted_sec_gcseg2.ave(),
                        weighted_sec_gcseg2_dis: weighted_sec_gcseg2_dis.ave(),
                        weighted_sec_of_educ: weighted_sec_of_educ.ave(),
                        weighted_sec_of_behaviour: weighted_sec_of_behaviour.ave(),
                        weighted_sec_of_overall: weighted_sec_of_overall.ave(),
                        weighted_sec_of_sixthform: weighted_sec_of_sixthform.ave(),

                        best_sec_gcseg2,
                        best_sec_gcseg2_dis,
                        best_sec_of_overall,

                        best_prim_of_overall, 
                        best_prim_rwm_ta, 
                        best_prim_rwm_ta_dis,
                    });
                }
            }
        } else {
            println!("No postcode location for: {}", &pcode);
        }
    }

    Ok(())
}

pub fn add_region<P1: AsRef<Path>, P2: AsRef<Path>>(input: P1, out: P2, regional_data: &HashMap<String, RegionRecord>) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(out)?;

    let mut rdr = ReaderBuilder::new()
    //.has_headers(true)
    //.flexible(true)
    .from_path(input)?;

    for record in rdr.deserialize::<ProcessedPcodeRecord>() {
        match record {
            Ok(r) => {
                let (region, pcode_area) = if let Some(area_code) = first_letters(&r.pcode) {
                    regional_data.get(&area_code).map_or((None, None), |x| (Some(x.region.clone()), Some(x.area_name.clone())))
                } else {
                    (None, None)
                };
                let new_r = RegionalProcessedPcodeRecord::new(r, region, pcode_area);
                writer.serialize(&new_r);
            },
            Err(e) => println!("Failed to open record: {}", e),
        }
    }
    Ok(())
}

pub fn valid_region(pcode: &str) -> bool {
    !(pcode.starts_with("LL") || pcode.starts_with("SY") || pcode.starts_with("LD") || pcode.starts_with("SA") || pcode.starts_with("NP") || pcode.starts_with("CF"))
}

pub fn remove_wales<P1: AsRef<Path>, P2: AsRef<Path>>(input: P1, out: P2) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(out)?;

    let mut rdr = ReaderBuilder::new()
    //.has_headers(true)
    //.flexible(true)
    .from_path(input)?;

    for record in rdr.deserialize::<ProcessedPcodeRecord>() {
        match record {
            Ok(r) => {
                if r.pcode.starts_with("LL") || r.pcode.starts_with("SY") || r.pcode.starts_with("LD") || r.pcode.starts_with("SA") || r.pcode.starts_with("NP") || r.pcode.starts_with("CF") {
                    println!("removing: {}", &r.pcode);
                } else {
                    writer.serialize(r);
                }
            },
            Err(e) => println!("Failed to open record: {}", e),
        }
    }
    Ok(())
}

pub fn run_atomic() -> Result<(), Box<dyn Error>> {
    let regional_data = load_regional_data("areas.csv")?;
    //add_region("england_atomic.csv", "england_reg_atomic.csv", &regional_data);
    let year_range = 2017..2024;
    let regions = load_regions("postcodes.csv")?;

    let postcodes = parse_postcodes("pdata.csv", &regions, year_range)?;
    println!("Parsed {} postcodes", postcodes.len());

    let sec_data: Vec<AggregateSchoolRecord> = load_school_data("all_sec.csv")?;
    println!("Loaded {} sec schools", sec_data.len());

    let prim_data: Vec<AggregatePSchoolRecord> = load_school_data("all_prim.csv")?;
    println!("Loaded {} prim schools", prim_data.len());

    let towns_data = parse_cities("towns.csv")?;
    println!("Loaded {} towns", towns_data.len());

    let cities_data = parse_cities("cities.csv")?;
    println!("Loaded {} cities", cities_data.len());

    let mut geo_data = load_geo_data("geo.csv")?;

    let mut sec_map: HashMap<u32, Vec<AggregateSchoolRecord>> = HashMap::new();
    println!("Loading sec school geo data - may take some time...");

    for sch in sec_data {
        if let Some(m) = sec_map.get_mut(&sch.year) {
            m.push(sch);
        } else {
            sec_map.insert(sch.year, vec![sch]);
        }
    }

    let mut prim_map: HashMap<u32, Vec<AggregatePSchoolRecord>> = HashMap::new();
    println!("Loading prim school geo data - may take some time...");

    for sch in prim_data {
        if let Some(m) = prim_map.get_mut(&sch.year) {
            m.push(sch);
        } else {
            prim_map.insert(sch.year, vec![sch]);
        }
    }

    let writer = Writer::from_path("full_atomic_async.csv")?;

    let writer_mx = Arc::new(Mutex::new(writer));
    let sec_map = Arc::new(sec_map);
    let prim_map = Arc::new(prim_map);
    let towns_data = Arc::new(towns_data);
    let cities_data = Arc::new(cities_data);
    let geo_data = Arc::new(geo_data);
    let regional_data = Arc::new(regional_data);

    let mut current_map = HashMap::new();
    let mut counter = 0;
    let mut max = postcodes.len() / 6;
    let fn_idx = postcodes.len() - 1;

    let mut handles = Vec::new();
    for (i, (k, v)) in postcodes.into_iter().enumerate() {
        if counter < max && i < fn_idx {
            current_map.insert(k, v);
            counter += 1;
        } else {
            let writer_mx = writer_mx.clone();
            let sec_map = sec_map.clone();
            let prim_map = prim_map.clone();
            let towns_data = towns_data.clone();
            let cities_data = cities_data.clone();
            let geo_data = geo_data.clone();
            let regional_data = regional_data.clone();
            handles.push(std::thread::spawn(move || {
                aggregate_pdata(writer_mx, current_map, sec_map, prim_map, towns_data, cities_data, geo_data, regional_data, 2017..2024);
            }));
            counter = 0;
            current_map = HashMap::new();
        }     
    }

    for handle in handles {
        handle.join();
    }

    Ok(())
}