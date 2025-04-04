use std::{collections::HashMap, io::Error};

pub type SymbolsMap = HashMap<String, String>;

pub fn load_symbols(filename: String) -> Result<SymbolsMap, Error> {
    let mut map = HashMap::new();
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b',')
        .quoting(false)
        .has_headers(false)
        .from_path(filename)
        .unwrap();
    for result in rdr.records() {
        let item = result?;
        let k = item.get(0).unwrap().to_string();
        let v = item.get(1).unwrap().to_string();
        map.insert(k, v);
    }
    Ok(map)
}
