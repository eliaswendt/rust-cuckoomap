pub const FINGERPRINT_SIZE: usize = 1;
// we define fingerprint 0 as empty
const EMPTY_FINGERPRINT: [u8; FINGERPRINT_SIZE] = [0; FINGERPRINT_SIZE];
pub const VALUE_SIZE: usize = 1;

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
    pub value: [u8; VALUE_SIZE]
}

impl Bucket {
    /// Creates a new bucket with a pre-allocated buffer.
    pub fn new() -> Self {
        Self {
            fingerprint: Fingerprint::empty(),
            value: [0; VALUE_SIZE] // just initalize with anything
        }
    }

    /// Sets the fingerprint of the `Bucket` if not full
    /// OR the fingerprint is the same.
    /// This operation is O(1).
    pub fn set(&mut self, fingerprint: Fingerprint, value: [u8; VALUE_SIZE]) -> bool {
    
        if self.fingerprint.is_empty() || self.fingerprint == fingerprint {
            self.fingerprint = fingerprint;
            self.value = value;
            return true;
        }
        false
    }

    /// Deletes the given fingerprint from the bucket. This operation is O(1).
    pub fn reset(&mut self, fingerprint: Fingerprint) -> bool {

        if self.fingerprint == fingerprint {
            self.fingerprint = Fingerprint::empty();
            // no need to invalidate data
            true
        } else {
            false
        }
    }

    pub fn clear(&mut self) {
        self.fingerprint = Fingerprint::empty()
    }
}