use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};

#[derive(Debug)]
struct Command {
    operation: String,
    arguments: Vec<String>,
}

fn parse_command(input: &str) -> Result<Command, Box<dyn Error>> {
    let parts: Vec<&str> = input.split_whitespace().collect();

    if parts.is_empty() {
        return Err("Empty input".into());
    }

    if parts.len() == 1 {
        return Err("Invalid input: missing arguments".into());
    }

    let operation = parts[0].to_string();
    let arguments: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();

    Ok(Command {
        operation: operation,
        arguments: arguments,
    })
}

fn handle_put(command: &Command, datafile: &mut File) -> std::io::Result<()> {
    command.arguments[0].as_bytes();

    datafile.write_all(command.arguments[0].as_bytes())?;
    datafile.write_all(" ".as_bytes());
    datafile.write_all(command.arguments[1].as_bytes())?;
    datafile.write_all("\n".as_bytes());

    datafile.flush();
    println!("Wrote {} to data.db", command.arguments[0]);
    Ok(())
}

fn handle_get(command: &Command, datafile: &mut File) -> std::io::Result<()> {
    let read_file = BufReader::new(datafile);

    for line_result in read_file.lines() {
        let line = line_result?;
        let mut parts = line.split_whitespace();

        if let Some(key) = parts.next() {
            if key == command.arguments[0] {
                println!("Found key: {}", key);

                if let Some(value) = parts.next() {
                    println!("Value: {}", value);
                }

                return Ok(());
            }
        }
    }

    println!("Key not found");

    Ok(())
}

fn main() {
    println!("MinkDB starting...");
    println!("Attempting to open data file");

    let mut datafile = match OpenOptions::new()
        .append(true)
        .create(true)
        .read(true)
        .open("data.db")
    {
        Ok(file) => file,
        Err(e) => {
            println!("Error opening data file: {}", e);
            return;
        }
    };

    let stdin = io::stdin();
    let input = &mut String::new();

    loop {
        input.clear();
        stdin.read_line(input);

        let command = match parse_command(input) {
            Ok(command) => command,
            Err(e) => {
                println!("Error: {}", e);
                continue;
            }
        };

        println!("Command: {:?}", command);
        if command.operation == "put" {
            handle_put(&command, &mut datafile);
        }

        if command.operation == "get" {
            handle_get(&command, &mut datafile);
        }
    }

    // let datafile = File::open("data.db");
}
