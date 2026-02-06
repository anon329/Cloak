use std::fs::File;
use std::collections::LinkedList;
use std::io::Write;
use rand::{rng, Rng};
use rand_distr::{Distribution, Zipf};

const MAX_ID: usize = 1000000;
const S: f32 = 1.0;
const NUM_REQS: usize = 1000000;
const WRITE_RATIO: f64 = 0.5;
const WORKLOAD_NAME: &str = "workload_zipf_temporal_WR_0.5.txt";

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

fn main() {
    let mut file = File::create(WORKLOAD_NAME).unwrap();
    let mut id_list = LinkedList::from_iter(0..MAX_ID);

    for i in 0..NUM_REQS {
        let id = sample_zipf(MAX_ID, S);
        let selected_id = linked_list_remove(&mut id_list, id);
        id_list.push_front(selected_id);
        // writeln!(file, "{}, {}, {:?}", selected_id, id, id_list).unwrap();
        //sample bool
        let is_write = rng().random_bool(WRITE_RATIO);
        if is_write {
            writeln!(file, "{} {}", selected_id, i).unwrap();
        } else {
            writeln!(file, "{}", selected_id).unwrap();
        }
    }
}