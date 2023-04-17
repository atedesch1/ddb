use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
};

struct KeyValueStore {
    store: HashMap<String, String>,
    log: BufWriter<File>,
}

impl KeyValueStore {
    fn new(log_path: &Path) -> Self {
        let mut store = HashMap::new();
        match File::open(log_path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                for line in reader.lines() {
                    let line = line.unwrap();
                    let words = line.trim().split(' ').collect::<Vec<&str>>();

                    match words[0] {
                        "INSERT" => store.insert(words[1].to_string(), words[2].to_string()),
                        "DELETE" => store.remove(words[1]),
                        _ => panic!("Invalid log entry: {}", line),
                    };
                }
            }
            Err(_) => {
                File::create(log_path).unwrap();
            }
        };
        let log = BufWriter::new(OpenOptions::new().append(true).open(log_path).unwrap());
        return KeyValueStore { store, log };
    }
    fn get(&self, key: &str) -> Option<&String> {
        return self.store.get(key);
    }
    fn set(&mut self, key: String, value: String) -> Option<String> {
        self.log
            .write_all(format!("INSERT {} {}\n", key, value).as_bytes());
        self.flush();
        return self.store.insert(key, value);
    }
    fn delete(&mut self, key: &str) -> Option<String> {
        self.log.write_all(format!("DELETE {}\n", key).as_bytes());
        self.flush();
        return self.store.remove(key);
    }
    fn flush(&mut self) -> std::io::Result<()> {
        return self.log.flush();
    }
}

fn main() {
    let mut store = KeyValueStore::new(Path::new("./log.txt"));

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
            _ => println!("invalid entry"),
        };
    }
}
