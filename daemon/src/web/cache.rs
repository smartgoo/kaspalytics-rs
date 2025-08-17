use moka::future::Cache;
use std::time::Duration;

pub type WebCache = Cache<String, String>;

pub fn create_web_cache(ttl_seconds: u64, max_memory_mb: u64) -> WebCache {
    let max_memory_bytes = max_memory_mb * 1024 * 1024; // Convert MB to bytes

    Cache::builder()
        .max_capacity(max_memory_bytes)
        .time_to_live(Duration::from_secs(ttl_seconds))
        .weigher(|key: &String, value: &String| {
            // Weight = memory usage of key + value in bytes
            (key.len() + value.len()) as u32
        })
        .build()
}

pub async fn get_cached_json(cache: &WebCache, key: &str) -> Option<String> {
    cache.get(key).await
}

pub async fn set_cached_json(cache: &WebCache, key: String, json_response: String) {
    cache.insert(key, json_response).await;
}

#[allow(dead_code)]
pub async fn remove_cached(cache: &WebCache, key: &str) {
    cache.invalidate(key).await;
}

#[allow(dead_code)]
pub async fn clear_cache(cache: &WebCache) {
    cache.invalidate_all();
}

#[allow(dead_code)]
pub fn cache_stats(cache: &WebCache) -> (u64, u64) {
    (cache.entry_count(), cache.weighted_size())
}

#[allow(dead_code)]
pub fn cache_memory_usage_mb(cache: &WebCache) -> f64 {
    cache.weighted_size() as f64 / (1024.0 * 1024.0)
}
