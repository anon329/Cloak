use std::fs::File;
use std::collections::LinkedList;
use std::io::Write;
use rand::{rng, Rng};
use rand_distr::{Distribution, Zipf};

const MAX_ID: usize = 1000000;
const S_START: f32 = 0.0;
const S_END: f32 = 2.05;
const S_STEP: f32 = 0.1;
const NUM_REQS: usize = 1000000;
const WRITE_RATIO: f64 = 0.5;
const WORKLOAD_NAME: &str = "benchmark/workload_zipf_temporal_WR_0.5_dist/";

pub fn sample_zipf(max_id: usize, s: f32) -> usize {
    // Zipf in rand_distr uses ranks starting at 1, so our n = max_id + 1
    let zipf = Zipf::new(max_id as f32, s)
        .expect("max_id must be ≥ 1 and s > 0");

    // Draw a rank in 1..=n and map to 0‑based id
    zipf.sample(&mut rng()) as usize - 1
}

pub fn linked_list_remove(id_list: &mut LinkedList<usize>, id: usize) -> usize {
    let mut split_list = id_list.split_off(id);
    let removed_id = split_list.pop_front().unwrap();
    id_list.append(&mut split_list);
    removed_id
}

fn move_to_front(id_list: &mut Vec<usize>, idx: usize) -> usize {
    let selected = id_list[idx];
    id_list.copy_within(0..idx, 1); // shift the prefix right by 1
    id_list[0] = selected;
    selected
}

fn main() {
    let mut s = S_START;
    while s <= S_END {
        let mut file = File::create(WORKLOAD_NAME.to_owned()+&format!("s{:.1}",s)).unwrap();
        let mut id_list: Vec<usize> = (0..MAX_ID).collect();
        let zipf = Zipf::new(MAX_ID as f32, s).unwrap();

        for i in 0..NUM_REQS {
            let id = zipf.sample(&mut rng()) as usize - 1;
            assert!(id < MAX_ID);
            let selected_id = move_to_front(&mut id_list, id);
            // writeln!(file, "{}, {}, {:?}", selected_id, id, id_list).unwrap();
            //sample bool
            let is_write = rng().random_bool(WRITE_RATIO);
            if is_write {
                writeln!(file, "{} {}", selected_id, i).unwrap();
            } else {
                writeln!(file, "{}", selected_id).unwrap();
            }
        }
        s += S_STEP;
    }
}