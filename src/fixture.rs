use rand::RngCore;

use super::PAGE_SIZE;

pub fn create_random_binary_page_data() -> [u8; PAGE_SIZE] {
    let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(1);
    let mut random_binary_data = [0u8; PAGE_SIZE];
    rng.fill_bytes(&mut random_binary_data);
    random_binary_data
}
