use rand::RngCore;

use super::PAGE_SIZE;
use std::fs;

pub fn create_random_binary_page_data() -> [u8; PAGE_SIZE] {
    let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(1);
    let mut random_binary_data = [0u8; PAGE_SIZE];
    rng.fill_bytes(&mut random_binary_data);
    random_binary_data
}

pub fn tear_down(db_file_name: &str) {
    let log_file_name = db_file_name
        .rfind('.')
        .map(|n| db_file_name[..n].to_string() + ".log")
        .unwrap();

    fs::remove_file(db_file_name).unwrap();
    fs::remove_file(log_file_name).unwrap();
}
