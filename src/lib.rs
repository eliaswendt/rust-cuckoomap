//! Cuckoo filter probabilistic data structure for membership testing and cardinality counting.
//!
//! # Usage
//!
//! This crate is [on crates.io](https://crates.io/crates/cuckoofilter) and can be
//! used by adding `cuckoofilter` to the dependencies in your project's `Cargo.toml`.
//!
//! ```toml
//! [dependencies]
//! cuckoofilter = "0.3"
//! ```
//!
//! And this in your crate root:
//!
//! ```rust
//! extern crate cuckoomap;
//! ```

mod bucket;
mod util;

use crate::bucket::{Bucket, Fingerprint, FINGERPRINT_SIZE};
use crate::util::{get_alt_index, get_fai, FaI};

use std::cmp;
use std::collections::hash_map::DefaultHasher;
use std::error::Error as StdError;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::repeat;
use std::marker::PhantomData;
use std::mem;

use bucket::VALUE_SIZE;
use rand::Rng;
#[cfg(feature = "serde_support")]
use serde_derive::{Deserialize, Serialize};

/// If insertion fails, we will retry this many times.
pub const MAX_REBUCKET: u32 = 500;

/// The default number of buckets.
pub const DEFAULT_CAPACITY: usize = (1 << 20) - 1;

#[derive(Debug)]
pub enum CuckooError {
    NotEnoughSpace,
}

impl fmt::Display for CuckooError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("NotEnoughSpace")
    }
}

impl StdError for CuckooError {
    fn description(&self) -> &str {
        "Not enough space to store this item, rebucketing failed."
    }
}

/// A cuckoo filter class exposes a Bloomier filter interface,
/// providing methods of add, delete, contains.
///
/// # Examples
///
/// ```
/// extern crate cuckoomap;
/// use cuckoomap::Value;
///
/// let words = vec!["foo", "bar", "xylophone", "milagro"];
/// let mut cf = cuckoomap::CuckooMap::new();
///
/// let mut insertions = 0;
/// for s in &words {
///     if cf.test_and_add(s, Value::new()).unwrap() {
///         insertions += 1;
///     }
/// }
///
/// assert_eq!(insertions, words.len());
/// assert_eq!(cf.len(), words.len());
///
/// // Re-add the first element.
/// cf.add(words[0], Value::new());
///
/// assert_eq!(cf.len(), words.len() + 1);
///
/// for s in &words {
///     cf.delete(s);
/// }
///
/// assert_eq!(cf.len(), 1);
/// assert!(!cf.is_empty());
///
/// cf.delete(words[0]);
///
/// assert_eq!(cf.len(), 0);
/// assert!(cf.is_empty());
///
/// for s in &words {
///     if cf.test_and_add(s, Value::new()).unwrap() {
///         insertions += 1;
///     }
/// }
///
/// cf.clear();
///
/// assert!(cf.is_empty());
///
/// ```
/// 

/// type for value of the key-value pair
/// gets saved inside an Entry together with the Key's Fingerprint
#[derive(Clone, Copy)]
pub struct Value(pub u8);

pub struct CuckooMap<H> {
    buckets: Box<[Bucket]>,
    len: usize,
    _hasher: std::marker::PhantomData<H>,
}

impl Default for CuckooMap<DefaultHasher> {
    fn default() -> Self {
        Self::new()
    }
}

impl CuckooMap<DefaultHasher> {
    /// Construct a CuckooFilter with default capacity and hasher.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }
}

impl<H> CuckooMap<H>
where
    H: Hasher + Default,
{
    /// Constructs a Cuckoo Filter with a given max capacity
    pub fn with_capacity(cap: usize) -> Self {
        let capacity = cmp::max(1, cap.next_power_of_two());

        Self {
            buckets: repeat(Bucket::new())
                .take(capacity)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            len: 0,
            _hasher: PhantomData,
        }
    }

    /// Checks if `key` is in the filter.
    /// returns `Some([u8; VALUE_SIZE])` if key probably is in the map
    /// returns `None` if key is definitely not in the map
    pub fn get<T: ?Sized + Hash>(&self, key: &T) -> Option<[u8; VALUE_SIZE]> {
        let FaI { fp, i1, i2 } = get_fai::<T, H>(key);
        let len = self.buckets.len();
        
        if self.buckets[i1 % len].fingerprint == fp {
            return Some(self.buckets[i1 % len].value)
        }

        if self.buckets[i2 % len].fingerprint == fp {
            return Some(self.buckets[i2 % len].value)
        }
        
        // not found
        None
    }

    /// Adds `key` along with a `value` to the filter. Returns `Ok` if the insertion was successful,
    /// but could fail with a `NotEnoughSpace` error, especially when the filter
    /// is nearing its capacity.
    /// Note that while you can put any hashable type in the same filter, beware
    /// for side effects like that the same number can have diferent hashes
    /// depending on the type.
    /// So for the filter, 4711i64 isn't the same as 4711u64.
    ///
    /// **Note:** When this returns `NotEnoughSpace`, the element given was
    /// actually added to the filter, but some random *other* element was
    /// removed. This might improve in the future.
    pub fn insert<T: ?Sized + Hash>(&mut self, key: &T, value: [u8; VALUE_SIZE]) -> Result<(), CuckooError> {
        let fai = get_fai::<T, H>(key);
        if self.put(fai.i1, fai.fp, value) || self.put(fai.i2, fai.fp, value) {
            return Ok(());
        }

        let len = self.buckets.len();
        let mut rng = rand::thread_rng();
        let mut i = fai.random_index(&mut rng);

        let mut current_bucket = Bucket {
            fingerprint: fai.fp,
            value: value
        };

        for _ in 0..MAX_REBUCKET {
            let kicked_bucket;
            {
                // save bucket that will get kicket out
                kicked_bucket = self.buckets[i % len];

                // save current_bucket into current position
                self.buckets[i % len] = current_bucket;

                // generate next position for kicked_bucket
                i = get_alt_index::<H>(kicked_bucket.fingerprint, i);
            }
            if self.put(i, kicked_bucket.fingerprint, kicked_bucket.value) {
                return Ok(());
            }
            current_bucket = kicked_bucket;
        }

        // TODO: consider resizing here

        // fp is dropped here, which means that the last item that was
        // rebucketed gets removed from the filter.
        // TODO: One could introduce a single-item cache for this element,
        // check this cache in all methods additionally to the actual filter,
        // and return NotEnoughSpace if that cache is already in use.
        // This would complicate the code, but stop random elements from
        // getting removed and result in nicer behaviour for the user.
        Err(CuckooError::NotEnoughSpace)
    }

    /// Adds `key` to the filter if it does not exist in the filter yet.
    /// Returns `Ok(true)` if `key` was not yet present in the filter and added
    /// successfully.
    pub fn test_and_add<T: ?Sized + Hash>(&mut self, key: &T, value: [u8; VALUE_SIZE]) -> Result<bool, CuckooError> {
        if self.get(key).is_some() {
            Ok(false)
        } else {
            self.insert(key, value).map(|_| true)
        }
    }

    /// Number of items in the filter.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Number of bytes the filter occupies in memory
    pub fn memory_usage(&self) -> usize {
        mem::size_of_val(self) + self.buckets.len() * mem::size_of::<Bucket>()
    }

    /// Check if filter is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Deletes `data` from the filter. Returns true if `data` existed in the
    /// filter before.
    pub fn delete<T: ?Sized + Hash>(&mut self, key: &T) -> bool {
        let FaI { fp, i1, i2 } = get_fai::<T, H>(key);
        self.remove(fp, i1) || self.remove(fp, i2)
    }

    /// Empty all the buckets in a filter and reset the number of items.
    pub fn clear(&mut self) {
        if self.is_empty() {
            return;
        }

        for bucket in self.buckets.iter_mut() {
            bucket.clear();
        }
        self.len = 0;
    }

    /// Removes the item with the given fingerprint from the bucket indexed by i.
    fn remove(&mut self, fp: Fingerprint, i: usize) -> bool {
        let len = self.buckets.len();
        if self.buckets[i % len].reset(fp) {
            self.len -= 1;
            true
        } else {
            false
        }
    }

    /// overwrites a bucket if fingerprint matches (prob. because of same key)
    fn put(&mut self, i: usize, fp: Fingerprint, value: [u8; VALUE_SIZE]) -> bool {
        let len = self.buckets.len();

        if self.buckets[i % len].set(fp, value) {
            self.len += 1;
            true
        } else {
            false
        }
    }

    /// calculates the the ratio of filled / empty buckets
    pub fn density(&self) -> f64 {

        let n_filled_buckets = self.buckets.iter()
            .filter(|b| !b.fingerprint.is_empty())
            .count();

        n_filled_buckets as f64 / self.buckets.len() as f64
    }
}
