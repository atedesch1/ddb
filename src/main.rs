mod wal;

use std::{
    collections::HashMap,
    io::{self, Write},
};

use wal::WriteAheadLog;

struct KeyValueStore {
    store: HashMap<String, String>,
    log: WriteAheadLog,
}

impl KeyValueStore {
    fn new(log_path: &str) -> Self {
        let mut store = HashMap::new();
        let mut log = WriteAheadLog::new(log_path);
        let log_contents = log.read();
        log_contents.iter().for_each(|wal_operation| {
            match wal_operation.operation.as_str() {
                "INSERT" => store.insert(
                    wal_operation.arguments[0].clone(),
                    wal_operation.arguments[1].clone(),
                ),
                "DELETE" => store.remove(&wal_operation.arguments[0]),
                _ => panic!("Unhandled"),
            };
        });
        return KeyValueStore { store, log };
    }
    fn get(&self, key: &str) -> Option<&String> {
        return self.store.get(key);
    }
    fn set(&mut self, key: String, value: String) -> Option<String> {
        self.log
            .append(format!("INSERT {} {}\n", key, value).as_str())
            .expect("operation couldn't be appended to wal");
        return self.store.insert(key, value);
    }
    fn delete(&mut self, key: &str) -> Option<String> {
        self.log
            .append(format!("DELETE {}\n", key).as_str())
            .expect("operation couldn't be appended to wal");
        return self.store.remove(key);
    }
    fn list(&self) -> Vec<(&String, &String)> {
        return self.store.iter().collect();
    }
}

fn main() {
    let mut store = KeyValueStore::new("./log.txt");

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let words = input.trim().split(' ').collect::<Vec<&str>>();
        let operation = words[0].to_uppercase();

        match &operation[..] {
            "INSERT" => {
                let key = words[1].to_string();
                let value = words[2].to_string();
                let result = store.set(key.clone(), value.clone());
                match result {
                    Some(prev) => println!("Previous value was: {}, inserting: {}", prev, value),
                    None => println!("Inserting new pair ({}, {})", key, value),
                };
            }
            "DELETE" => {
                let result = store.delete(words[1]);
                match result {
                    Some(prev) => println!("Deleting: ({}, {})", words[1], prev),
                    None => println!("Key not present"),
                };
            }
            "GET" => {
                let result = store.get(words[1]);
                match result {
                    Some(val) => println!("Value is: {}", val),
                    None => println!("Key not present"),
                };
            }
            "LIST" => {
                let result = store.list();
                println!("Listing KeyValue pairs:");
                for (k, v) in result {
                    println!("{}:{}", k, v);
                }
            }
            _ => println!("invalid entry"),
        };
    }
}
