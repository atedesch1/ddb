use std::str::FromStr;
use std::{
    fs::{File, OpenOptions},
    io::{Error, Read, Write},
};

pub enum WalOperation {
    Insert,
    Delete,
}

impl FromStr for WalOperation {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "insert" => Ok(WalOperation::Insert),
            "delete" => Ok(WalOperation::Delete),
            _ => Err(Error::new(std::io::ErrorKind::Other, "invalid operation")),
        }
    }
}

pub struct WalOperationLog {
    pub operation: WalOperation,
    pub key: String,
    pub value: Option<String>,
}

impl FromStr for WalOperationLog {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let words = s.trim().split(' ').collect::<Vec<&str>>();
        let operation = match words.get(0) {
            Some(operation) => WalOperation::from_str(operation)?,
            None => return Err(Error::new(std::io::ErrorKind::Other, "missing operation")),
        };
        let key = match words.get(1) {
            Some(key) => key.to_string(),
            None => return Err(Error::new(std::io::ErrorKind::Other, "missing key")),
        };
        let value = match words.get(2) {
            Some(value) => Some(value.to_string()),
            None => None,
        };

        return Ok(WalOperationLog {
            operation,
            key,
            value,
        });
    }
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

    pub fn read(&mut self) -> Result<Vec<WalOperationLog>, std::io::Error> {
        let mut contents = String::new();
        self.log.read_to_string(&mut contents)?;
        return contents
            .lines()
            .map(|line| -> Result<WalOperationLog, std::io::Error> {
                WalOperationLog::from_str(line)
            })
            .collect();
    }

    pub fn append(&mut self, content: &str) -> Result<(), Error> {
        self.log.write_all(content.as_bytes())
    }
}
