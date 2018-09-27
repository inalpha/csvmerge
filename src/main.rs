extern crate serde;

#[macro_use]
extern crate serde_derive;

extern crate config;
extern crate csv;

use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

// fn get_header_indices() {

// }
mod settings;

use settings::Settings;

fn main() -> Result<(), Box<Error>> {
    let settings = Settings::new().unwrap();
    let filenames: Vec<String> = env::args().skip(1).collect();
    let mut wtr = csv::Writer::from_writer(fs::File::create("output.csv")?);
    let mut output_header = csv::ByteRecord::new();
    let columns_count = settings.columns.len();
    for c in settings.columns {
        output_header.push_field(c.label.as_bytes());
    }
    wtr.write_byte_record(&output_header)?;

    for file in filenames {
        let path = Path::new(&file);
        let file = fs::File::open(path)?;
        let mut rdr = csv::ReaderBuilder::new().from_reader(file);
        let idx = {
            let headers = rdr.byte_headers()?;
            let mut idx: Vec<Option<usize>> = vec![None; columns_count];
            let mut i: usize;
            let mut j: usize = 0;
            for field in headers.iter() {
                i = 0;
                for f in output_header.iter() {
                    if field == f {
                        idx[i] = Some(j);
                    }
                    i += 1;
                }
                j += 1;
            }
           idx
        };

        let mut row = csv::ByteRecord::new();

        while rdr.read_byte_record(&mut row)? {
            let mut roww = csv::ByteRecord::new();
            for i in idx.clone() {
                roww.push_field(match i {
                    None => b"",
                    Some(x) => &row.get(x).unwrap(),
                })
            }
            wtr.write_byte_record(&roww)?;
        }
    }
    wtr.flush()?;
    Ok(())
}