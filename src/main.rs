use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};

struct MinkDB {
    datafile: File,
    index: HashMap<String, u64>,
}

impl MinkDB {
    fn new(path: &str) -> io::Result<Self> {
        let datafile = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(path)?; // if it fails, it returns some Result Error thing

        let mut db = MinkDB {
            datafile: datafile,
            index: HashMap::new(),
        };

        db.build_index()?; // we must implement this 
        Ok(db)
    }

    fn build_index(&mut self) -> io::Result<()> {
        self.datafile.seek(SeekFrom::Start(0))?; // seek to the beginning of the file

        let mut reader = BufReader::new(&self.datafile);
        let mut offset: u64 = 0;

        loop {
            let mut line = String::new();
            let bytes_read = reader.read_line(&mut line)?; // read a line from the file

            // bit weird that it doesn't return a result, but returns the number of bytes read
            // whereas the results are stored in the line variable

            if bytes_read == 0 {
                // reached the end of the file
                break; // loop breaking only here, once we get EOF
            }

            if let Some(key) = line.split_whitespace().next() {
                self.index.insert(key.to_string(), offset); // we just want the keys and the
                // offsets, okay we know them
            }

            offset += bytes_read as u64;
        }

        Ok(())
    }

    fn handle_put(&mut self, command: &Command) -> io::Result<()> {
        let offset = self.datafile.seek(SeekFrom::End(0))?; // get the offset of the end of the
        // file
        let key = &command.arguments[0];
        let value = &command.arguments[1];

        writeln!(self.datafile, "{} {}", key, value)?; // why didn't i have to write the
        // key-length, value-length, and the key and value separately?

        self.datafile.flush()?;
        self.index.insert(key.to_string(), offset);

        println!("Wrote {} to data.db", key);
        Ok(())
    }

    fn handle_get(&mut self, command: &Command) -> io::Result<()> {
        let key = &command.arguments[0];

        match self.index.get(key) {
            Some(&offset) => {
                self.datafile.seek(SeekFrom::Start(offset))?;

                let mut reader = BufReader::new(&self.datafile);
                let mut line = String::new();

                reader.read_line(&mut line)?;

                let mut parts = line.split_whitespace();
                parts.next();

                if let Some(value) = parts.next() {
                    println!("Value: {}", value);
                }
            }

            None => {
                println!("Key not found");
            }
        }

        Ok(())
    }
}

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

    Ok(())
}

fn main() {
    println!("MinkDB starting...");
    println!("Attempting to open data file");

    let mut db = match MinkDB::new("data.db") {
        Ok(db) => {
            println!("Loaded {} keys from file", db.index.len());
            db
        }

        Err(e) => {
            println!("Error opening database: {}", e);
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

        match command.operation.as_str() {
            "put" => db.handle_put(&command),
            "get" => db.handle_get(&command),
            _ => {
                println!("Invalid operaiton {}", command.operation);
                Ok(())
            }
        };
    }
}
