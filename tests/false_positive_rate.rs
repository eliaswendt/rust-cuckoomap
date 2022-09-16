use cuckoomap::{CuckooMap, Value};

use std::collections::hash_map::DefaultHasher;

// Modelled after
// https://github.com/efficient/cuckoofilter/blob/master/example/test.cc
// to make test setup and results comparable.

#[test]
fn false_positive_rate() {
    let total_items = 1_000_000;

    let mut filter = CuckooMap::<DefaultHasher>::with_capacity(total_items);

    let mut num_inserted: u64 = 0;
    // We might not be able to get all items in, but still there should be enough
    // so we can just use what has fit in and continue with the test.
    for i in 0..total_items {
        match filter.insert(&i, [0; 1]) {
            Ok(_) => num_inserted += 1,
            Err(_) => break,
        }
    }

    // The range 0..num_inserted are all known to be in the filter.
    // The filter shouldn't return false negatives, and therefore they should all be contained.
    for i in 0..num_inserted {
        assert!(filter.get(&i).is_some());
    }

    // The range total_items..(2 * total_items) are all known *not* to be in the filter.
    // Every element for which the filter claims that it is contained is therefore a false positive.
    let mut false_queries: u64 = 0;
    for i in total_items..(2 * total_items) {
        if filter.get(&i).is_some() {
            false_queries += 1;
        }
    }
    let false_positive_rate = (false_queries as f64) / (total_items as f64);

    println!("elements inserted: {}", num_inserted);
    println!(
        "memory usage: {:.2}KiB",
        (filter.memory_usage() as f64) / 1024.0
    );
    println!("false positive rate: {}%", 100.0 * false_positive_rate);
    // ratio should be around 0.024, round up to 0.03 to accomodate for random fluctuation
    assert!(false_positive_rate < 0.03);
}
