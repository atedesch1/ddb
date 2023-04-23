use core::fmt;
use std::str::FromStr;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
};

pub enum WalOperation {
    Insert,
    Delete,
}

#[derive(Debug)]
pub enum ParseWalError {
    MissingOperation,
    InvalidOperation(String),
    MissingKey,
    MissingValue,
}

impl fmt::Display for ParseWalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WAL operation error: Could not parse operation.")
    }
}

impl std::error::Error for ParseWalError {}

impl FromStr for WalOperation {
    type Err = ParseWalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "insert" => Ok(WalOperation::Insert),
            "delete" => Ok(WalOperation::Delete),
            operation => Err(ParseWalError::InvalidOperation(operation.to_owned())),
        }
    }
}

pub struct WalOperationLog {
    pub operation: WalOperation,
    pub key: String,
    pub value: Option<String>,
}

impl FromStr for WalOperationLog {
    type Err = ParseWalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let words = s.trim().split(' ').collect::<Vec<&str>>();
        let operation = match words.get(0) {
            Some(operation) => WalOperation::from_str(operation)?,
            None => return Err(ParseWalError::MissingOperation),
        };
        let key = match words.get(1) {
            Some(key) => key.to_string(),
            None => return Err(ParseWalError::MissingKey),
        };
        let value = match words.get(2) {
            Some(value) => Some(value.to_string()),
            None => match operation {
                WalOperation::Insert => return Err(ParseWalError::MissingValue),
                WalOperation::Delete => None,
            },
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

    pub fn read(&mut self) -> Result<Vec<WalOperationLog>, ParseWalError> {
        let mut contents = String::new();
        self.log.read_to_string(&mut contents).unwrap();
        return contents
            .lines()
            .map(|line| -> Result<WalOperationLog, ParseWalError> {
                WalOperationLog::from_str(line)
            })
            .collect();
    }

    pub fn log_insert_operation(&mut self, key: &str, value: &str) -> () {
        self.append(format!("INSERT {} {}\n", key, value).as_str())
            .unwrap();
    }

    pub fn log_delete_operation(&mut self, key: &str) -> () {
        self.append(format!("DELETE {}\n", key).as_str()).unwrap();
    }

    fn append(&mut self, content: &str) -> Result<(), std::io::Error> {
        self.log.write_all(content.as_bytes())
    }
}
