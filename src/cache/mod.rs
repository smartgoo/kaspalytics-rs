pub mod models;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub struct Cache<K, V> {
    cache: Arc<Mutex<BTreeMap<K, V>>>,
}

impl<K, V> Cache<K, V>
where
    K: Ord,
{
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        let mut cache = self.cache.lock().unwrap();
        cache.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let cache = self.cache.lock().unwrap();
        cache.get(key).cloned()
    }

    pub fn update(&self, key: &K, new_value: V) {
        let mut cache = self.cache.lock().unwrap();
        if let Some(curr_value) = cache.get_mut(key) {
            *curr_value = new_value;
        }
    }

    pub fn remove(&self, key: &K) {
        let mut cache = self.cache.lock().unwrap();
        cache.remove(key);
    }

    pub fn entry_count(&self) -> u64 {
        let cache = self.cache.lock().unwrap();
        cache.len() as u64
    }
}
