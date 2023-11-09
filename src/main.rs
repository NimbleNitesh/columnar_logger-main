use std::io::{BufRead, BufReader, Result, Write};
use serde_json::Value;
use std::fs::File;
use std::collections::{HashMap, VecDeque};
use rayon::prelude::*;

// Change it as per your system's RAM
const LIMIT: u64 = 6_000_000;

// Function to recursively traverse JSON data and accumulate it in a HashMap
fn traverse_json(
    prefix: &str,
    value: &Value,
    column_files: &mut HashMap<String, VecDeque<String>>,
) -> Result<()> {
    match value {
        Value::Object(obj) => {
            for (key, val) in obj {
                let new_prefix = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                traverse_json(&new_prefix, val, column_files)?;
            }
        }
        Value::String(s) => {
            // Add the string value to the HashMap under the specified prefix
            if let Some(queue) = column_files.get_mut(prefix) {
                queue.push_back(s.to_string());
            } else {
                let mut queue = VecDeque::new();
                queue.push_back(s.to_string());
                column_files.insert(prefix.to_string(), queue);
            }
        }
        Value::Number(n) => {
            // Add the number value to the HashMap under the specified prefix
            let s = n.to_string();
            if let Some(queue) = column_files.get_mut(prefix) {
                queue.push_back(s.to_string());
            } else {
                let mut queue = VecDeque::new();
                queue.push_back(s.to_string());
                column_files.insert(prefix.to_string(), queue);
            }
        }
        Value::Bool(b) => {
            // Add the boolean value to the HashMap under the specified prefix
            let s = if *b { "true" } else { "false" };
            if let Some(queue) = column_files.get_mut(prefix) {
                queue.push_back(s.to_string());
            } else {
                let mut queue = VecDeque::new();
                queue.push_back(s.to_string());
                column_files.insert(prefix.to_string(), queue);
            }
        }
        Value::Null => {
            // Add null to the HashMap under the specified prefix
            let s = "";
            if let Some(queue) = column_files.get_mut(prefix) {
                queue.push_back(s.to_string());
            } else {
                let mut queue = VecDeque::new();
                queue.push_back(s.to_string());
                column_files.insert(prefix.to_string(), queue);
            }
        }
        _ => (),
    }

    Ok(())
}

// Function to write accumulated data to output files in parallel
fn write_to_files(output_folder: &str, column_files: &HashMap<String, VecDeque<String>>) {
    // Set the number of threads explicitly
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(8) // Set the number of threads according to your CPU cores
        .build_global();

    // Use Rayon's parallel iterators for more control
    column_files.par_iter().for_each(|(key, value)| {
        // let file = File::create(format!("{}/{}.column", output_folder, key))
        //     .expect("Failed to create file");
        let file_path = format!("{}/{}.column", output_folder, key);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true) // Open the file in append mode
            .open(&file_path)
            .expect("Failed to open or create file");

        let mut writer = std::io::BufWriter::new(file);
        for val in value {
            writeln!(writer, "{}", val).expect("Failed to write to file");
        }
    });
}

fn main() -> Result<()> {
    println!("Hello, world!");
    println!("Please Enter the path of input file");
    let mut input_file_path = String::new();
    std::io::stdin().read_line(&mut input_file_path).expect("Failed to read line");
    println!("Please enter the path of the output folder");
    let mut output_folder_path = String::new();
    std::io::stdin().read_line(&mut output_folder_path).expect("Failed to read line");

    let start_time = std::time::Instant::now();

    let input_file = File::open(input_file_path.trim()).expect("Failed to open input file");
    let output_folder = output_folder_path.trim();
    let reader = BufReader::new(input_file);

    let mut column_files: HashMap<String, VecDeque<String>> = HashMap::new();
    let mut line_cnt = 0;

    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        let json: Value = serde_json::from_str(&line).expect("Failed to parse JSON");
        traverse_json("", &json, &mut column_files)?;
        line_cnt+=1;
        if line_cnt == LIMIT {
            write_to_files(output_folder, &column_files);
            line_cnt = 0;
            column_files.clear();
        }
            
    }

    if line_cnt > 0 {
        write_to_files(output_folder, &column_files);
    }
    
    let elapsed_time = start_time.elapsed();
    println!("Duration: {:?}", elapsed_time);

    Ok(())
}
