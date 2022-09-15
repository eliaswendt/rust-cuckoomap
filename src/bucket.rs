use crate::Value;

pub const FINGERPRINT_SIZE: usize = 1;
const EMPTY_FINGERPRINT: [u8; FINGERPRINT_SIZE] = [100; FINGERPRINT_SIZE];

// Fingerprint Size is 1 byte so lets remove the Vec
#[derive(PartialEq, Copy, Clone, Hash)]
pub struct Fingerprint {
    pub data: [u8; FINGERPRINT_SIZE],
}

impl Fingerprint {
    /// Attempts to create a new Fingerprint based on the given
    /// number. If the created Fingerprint would be equal to the
    /// empty Fingerprint, None is returned.
    pub fn from_data(data: [u8; FINGERPRINT_SIZE]) -> Option<Self> {
        let result = Self { data };
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Returns the empty Fingerprint.
    pub fn empty() -> Self {
        Self {
            data: EMPTY_FINGERPRINT,
        }
    }

    /// Checks if this is the empty Fingerprint.
    pub fn is_empty(&self) -> bool {
        self.data == EMPTY_FINGERPRINT
    }

    /// Sets the fingerprint value to a previously exported one via an in-memory copy.
    fn slice_copy(&mut self, fingerprint: &[u8]) {
        self.data.copy_from_slice(fingerprint);
    }
}


#[derive(Clone, Copy)]
pub struct Bucket {
    pub fingerprint: Fingerprint,
    pub value: Value
}

impl Bucket {
    /// Creates a new bucket with a pre-allocated buffer.
    pub fn new() -> Self {
        Self {
            fingerprint: Fingerprint::empty(),
            value: Value(0)
        }
    }

    /// Inserts the fingerprint into the `Bucket` if not full, OR 
    /// the fingerprint is the same.
    /// This operation is O(1).
    pub fn insert(&mut self, fingerprint: Fingerprint, value: Value) -> bool {
    
        if self.fingerprint.is_empty() || self.fingerprint == fingerprint {
            self.fingerprint = fingerprint;
            self.value = value;
            return true;
        }
        false
    }

    /// Deletes the given fingerprint from the bucket. This operation is O(1).
    pub fn delete(&mut self, fingerprint: Fingerprint) -> bool {

        if self.fingerprint == fingerprint {
            self.fingerprint = Fingerprint::empty();
            // no need to invalidate data
            true
        } else {
            false
        }
    }

    /// Returns all current fingerprint data of the current buffer for storage.
    pub fn get_fingerprint_data(&self) -> Vec<u8> {
        self.fingerprint.data.to_vec()
    }

    /// Empties the bucket by setting each used entry to Fingerprint::empty(). Returns the number of entries that were modified.
    #[inline(always)]
    pub fn clear(&mut self) {
        *self = Self::new()
    }
}

impl From<&[u8]> for Bucket {
    /// Constructs a buffer of fingerprints from a set of previously exported fingerprints.
    fn from(fingerprint: &[u8]) -> Self {

        let mut new_fingerprint = Fingerprint::empty();
        new_fingerprint.slice_copy(fingerprint);

        Self {
            fingerprint: new_fingerprint,
            value: Value(0) // TODO: also import Values
        }
    }
}
