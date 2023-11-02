use std::io::{BufRead, BufReader, Result, Write};
use serde_json::Value;
use std::fs::File;
use std::collections::{HashMap, VecDeque};
// use std::thread; // Import the thread module
use rayon::prelude::*;


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
            // if let Some(file) = column_files.get_mut(prefix) {
            //     writeln!(file, "{}", s)?;
            // } else {
            //     let mut file = File::create(&format!("{}/{}.column", output_folder, prefix))?;
            //     writeln!(file, "{}", s)?;
            //     column_files.insert(prefix.to_string(), file);
            // }
            if let Some(queue) = column_files.get_mut(prefix) {
                queue.push_back(s.to_string());
            } else {
                let mut queue = VecDeque::new();
                queue.push_back(s.to_string());
                column_files.insert(prefix.to_string(), queue);
                
            }
        }
        Value::Number(n) => {
            // if let Some(file) = column_files.get_mut(prefix) {
            //     writeln!(file, "{}", n)?;
            // } else {
            //     let mut file = File::create(&format!("{}/{}.column", output_folder, prefix))?;
            //     writeln!(file, "{}", n)?;
            //     column_files.insert(prefix.to_string(), file);
            // }
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
            // if let Some(file) = column_files.get_mut(prefix) {
            //     writeln!(file, "{}", b)?;
            // } else {
            //     let mut file = File::create(&format!("{}/{}.column", output_folder, prefix))?;
            //     writeln!(file, "{}", b)?;
            //     column_files.insert(prefix.to_string(), file);
            // }
            let s = if *b {"true"} else {"false"};
            if let Some(queue) = column_files.get_mut(prefix) {
                queue.push_back(s.to_string());
            } else {
                let mut queue = VecDeque::new();
                queue.push_back(s.to_string());
                column_files.insert(prefix.to_string(), queue);
                
            }
        }
        Value::Null => {
            // if let Some(file) = column_files.get_mut(prefix) {
            //     writeln!(file, "")?;
            // } else {
            //     let mut file = File::create(&format!("{}/{}.column", output_folder, prefix))?;
            //     writeln!(file, "")?;
            //     column_files.insert(prefix.to_string(), file);
            // }
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

// Using Threads
// fn write_to_files(output_folder: &str, column_files: &HashMap<String, VecDeque<String>>) {
//     let threads: Vec<_> = column_files.iter().map(|(key, value)| {
//         let output_folder = output_folder.to_string();
//         let key = key.to_string();
//         let value = value.clone();

//         thread::spawn(move || {
//             let mut file = File::create(format!("{}/{}.column", output_folder, key)).expect("Failed to create file");
//             for val in value {
//                 writeln!(file, "{}", val).expect("Failed to write to file");
//             }
//         })
//     }).collect();

//     for thread in threads {
//         thread.join().expect("Thread panicked");
//     }
// }

fn write_to_files(output_folder: &str, column_files: &HashMap<String, VecDeque<String>>) {
    // Set the number of threads explicitly
    rayon::ThreadPoolBuilder::new()
        .num_threads(8) // Set the number of threads according to your CPU cores
        .build_global()
        .unwrap();

    // Use Rayon's parallel iterators for more control
    column_files.par_iter().for_each(|(key, value)| {
        let file = File::create(format!("{}/{}.column", output_folder, key)).expect("Failed to create file");

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
    println!("Please enter the path of output folder");
    let mut output_folder_path = String::new();
    std::io::stdin().read_line(&mut output_folder_path).expect("Failed to read line");

    let start_time = std::time::Instant::now();

    let input_file = File::open(input_file_path.trim()).expect("Failed to open input file");
    let output_folder = output_folder_path.trim();
    let reader = BufReader::new(input_file);

    let mut column_files: HashMap<String, VecDeque<String>> = HashMap::new();

    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        let json: Value = serde_json::from_str(&line).expect("Failed to parse JSON");
        traverse_json("", &json, &mut column_files)?;
    }

    /*
    Plan is that we create multiple threads and each thread will write to a file
    file name will be the key of the hashmap
    value will be the queue. each value of queue will be written in a new line.
     */

    /*
    // BRUTE FORCE APPROACH

    for (key, value) in column_files {
        let mut file = File::create(&format!("{}/{}.column", output_folder, key))?;
        for val in value {
            writeln!(file, "{}", val)?;
        }
    }
     */

    // Pass the HashMap to the function to write to files in parallel
    write_to_files(output_folder, &column_files);
    
    let elapsed_time = start_time.elapsed();
    println!("Duration: {:?}", elapsed_time);

    Ok(())
}
