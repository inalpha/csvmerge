extern crate num_cpus;
extern crate threadpool;

extern crate serde;

#[macro_use]
extern crate serde_derive;

extern crate config;
extern crate csv;

use std::collections::HashSet;
use std::{env, fs, str};
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::channel;
use threadpool::ThreadPool;
mod settings;

use settings::Settings;

pub struct Input {
    indices: Vec<Option<usize>>,
    rdr: csv::Reader<std::fs::File>,
    row: csv::ByteRecord,
}

impl Input {
    pub fn new(path: &str, columns: &Vec<Vec<String>>) -> Self {
        let path = Path::new(path);
        let file = fs::File::open(path).unwrap();
        let mut rdr = csv::ReaderBuilder::new().from_reader(file);
        let mut indices: Vec<Option<usize>> = vec![None; columns.len()];

        {
            let headers = rdr.byte_headers().unwrap();
            for (i, header) in headers.iter().enumerate() {
                for (j, matches) in columns.iter().enumerate() {
                    if matches.contains(&str::from_utf8(header).unwrap().to_string()) {
                        indices[j] = Some(i);
                    }
                }
            }
        }

        Input {
            indices: indices,
            rdr: rdr,
            row: csv::ByteRecord::new(),
        }
    }

    pub fn next(&mut self) -> Option<csv::ByteRecord> {
        match self.rdr.read_byte_record(&mut self.row) {
            Ok(true) => {
                let mut row = csv::ByteRecord::new();
                for i in &self.indices {
                    row.push_field(match i {
                        None => b"",
                        Some(i) => &self.row.get(*i).unwrap(),
                    })
                }
                Some(row)
            }
            Ok(false) => None,
            Err(_) => None,
        }
    }
}

pub struct Output {
    w: csv::Writer<std::fs::File>,
    c: HashSet<String>,
}

impl Output {
    pub fn new(path: &str, header: &csv::ByteRecord) -> Self {
        let mut w = csv::Writer::from_writer(fs::File::create(path).unwrap());
        w.write_byte_record(header).unwrap();
        w.flush().unwrap();
        Output {
            w: w,
            c: HashSet::new(),
        }
    }

    pub fn write(&mut self, row: &csv::ByteRecord) {
        let unique = str::from_utf8(&row.get(0).unwrap()).unwrap().to_string();
        if !self.c.contains(&unique) {
            self.c.insert(unique);
            self.w.write_byte_record(row).unwrap();
        }
    }

    pub fn flush(&mut self) {
        self.w.flush().unwrap();
    }
}

fn main() -> Result<(), Box<Error>> {
    let settings = Settings::new().unwrap();
    let filenames: Vec<String> = env::args().skip(1).collect();

    let mut header = csv::ByteRecord::new();
    let mut columns: Vec<Vec<String>> = vec![];
    for c in settings.columns {
        header.push_field(c.label.as_bytes());
        columns.push(c.matches);
    }

    let mut output = Output::new("output.csv", &header);
    let pool = ThreadPool::new(num_cpus::get());
    let (sender, receiver) = channel();
    let mut count = filenames.len();

    for file in filenames {
        let mut input = Input::new(&file, &columns);
        let sender = sender.clone();
        pool.execute(move|| {
            loop {
                match input.next() {
                    Some(r) => sender.send(Some(r)).unwrap(),
                    None => {
                        sender.send(None).unwrap();
                        break;
                    },
                }
            }
            println!("done with: {}", file);
        });
    }

    let mut iter = receiver.iter();
    loop {
        match iter.next() {
            Some(r) => match r {
                Some(r) => output.write(&r),
                None => {
                    count -= 1;
                    if count == 0 {
                        break;
                    }
                }
            },
            None => break,
        }
    }
    output.flush();

    println!("done");
    
    Ok(())
}
