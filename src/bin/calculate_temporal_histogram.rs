use std::fs::File;
use std::io::{Write, BufRead};


const WORKLOAD_NAME: &str = "benchmark/list_of_movie_queries.txt";
const DATASET_SIZE: usize = 8472;
const TRACE_SIZE: usize = 671736;

fn main() {

    let mut time_map = vec![0u32; DATASET_SIZE+1];
    let mut temporal_histogram = vec![0u32; TRACE_SIZE];

    let file = File::open(WORKLOAD_NAME).expect("Unable to open workload file");
    let reader = std::io::BufReader::new(file);

    let mut current_time = 0u32;
    for line in reader.lines() {
        current_time += 1;
        let line = line.expect("Unable to read line");
        let id = line.parse::<usize>().expect("Unable to parse line");
        let difference = current_time - time_map[id];
        if time_map[id] != 0 {
            temporal_histogram[difference as usize - 1] += 1;
        }
        time_map[id] = current_time;
    }

    let mut output_file = File::create("temporal_histogram.txt").expect("Unable to create output file");
    for count in temporal_histogram {
        writeln!(output_file, "{}", count).expect("Unable to write to output file");
    }
}


