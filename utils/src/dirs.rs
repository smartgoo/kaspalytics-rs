use crate::config::Env;
use kaspa_consensus_core::network::NetworkId;
use std::path::PathBuf;

const LOG_DIR: &str = "logs";
const CACHE_DIR: &str = "cache";
const DB_DIR: &str = "db.duckdb";

#[allow(dead_code)]
#[derive(Clone)]
pub struct KaspalyticsDirs {
    app_dir: PathBuf,
    env_dir: PathBuf,
    pub network_dir: PathBuf,
    pub log_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub db_dir: PathBuf,
}

impl KaspalyticsDirs {
    pub fn new(env: Env, network_id: NetworkId, app_dir: PathBuf) -> Self {
        let env_dir = app_dir.join(env.to_string());
        let network_dir = env_dir.join(network_id.to_string());
        std::fs::create_dir_all(&network_dir).unwrap();

        let log_dir = network_dir.join(LOG_DIR);
        std::fs::create_dir_all(&log_dir).unwrap();

        let cache_dir = network_dir.join(CACHE_DIR);
        std::fs::create_dir_all(&cache_dir).unwrap();
        
        let db_dir = network_dir.join(DB_DIR);

        Self {
            app_dir,
            env_dir,
            network_dir,
            log_dir,
            cache_dir,
            db_dir,
        }
    }
}
