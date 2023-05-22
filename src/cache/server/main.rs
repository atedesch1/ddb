use ddb::{cache::kv::Store, error::Result};
use std::{
    env,
    io::{self, Write},
};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let is_master = args
        .iter()
        .find(|&arg| (*arg).to_uppercase() == "MASTER")
        .map(|_| true)
        .unwrap_or(false);

    let mut store = Store::new().await?;

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let words = input.trim().split(' ').collect::<Vec<&str>>();
        let operation = words[0].to_uppercase();

        match &operation[..] {
            "INSERT" if is_master => {
                let key = words[1].to_string();
                let value = words[2].to_string();
                let result = store.set(key.as_bytes(), value.as_bytes()).await?;
                match result {
                    Some(prev) => println!(
                        "Previous value was: {}, inserting: {}",
                        String::from_utf8(prev).unwrap(),
                        value
                    ),
                    None => println!("Inserting new pair ({}, {})", key, value),
                };
            }
            "DELETE" if is_master => {
                let key = words[1].to_string();
                let result = store.delete(key.as_bytes()).await?;
                match result {
                    Some(prev) => {
                        println!("Deleting: ({}, {})", key, String::from_utf8(prev).unwrap())
                    }
                    None => println!("Key not present"),
                };
            }
            "GET" => {
                let key = words[1].to_string();
                let result = store.get(key.as_bytes());
                match result {
                    Some(val) => println!("Value is: {}", String::from_utf8(val.clone()).unwrap()),
                    None => println!("Key not present"),
                };
            }
            "LIST" => {
                let result = store.list();
                println!("Listing KeyValue pairs:");
                for (k, v) in result {
                    println!(
                        "{}:{}",
                        String::from_utf8(k.clone()).unwrap(),
                        String::from_utf8(v.clone()).unwrap()
                    );
                }
            }
            "EXIT" => return Ok(()),
            _ => println!("invalid entry"),
        };
    }
}
