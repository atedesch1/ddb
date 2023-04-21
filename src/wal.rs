use std::{
    fs::{File, OpenOptions},
    io::{Error, Read, Write},
};

pub struct WalOperation {
    pub operation: String,
    pub arguments: Vec<String>,
}

pub struct WriteAheadLog {
    log: File,
}

impl WriteAheadLog {
    pub fn new(path: &str) -> Self {
        if let Err(_) = File::open(path) {
            File::create(path).unwrap();
        }
        let log = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .unwrap();
        return WriteAheadLog { log };
    }

    pub fn read(&mut self) -> Vec<WalOperation> {
        let mut contents = String::new();
        self.log.read_to_string(&mut contents).unwrap();
        return contents
            .lines()
            .map(|line| {
                let words = line.trim().split(' ').collect::<Vec<&str>>();
                return WalOperation {
                    operation: words[0].to_owned(),
                    arguments: words[1..].iter().map(|&s| s.to_owned()).collect(),
                };
            })
            .collect();
    }

    pub fn append(&mut self, content: &str) -> Result<(), Error> {
        self.log.write_all(content.as_bytes())
    }
}
