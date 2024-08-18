use crate::args::Args;
use kaspa_consensus_core::network::{self, NetworkId};
// use kaspad_lib::daemon::{get_app_dir, CONSENSUS_DB, DEFAULT_DATA_DIR, META_DB, UTXOINDEX_DB};
use std::path::PathBuf;


pub fn get_home_dir() -> PathBuf {
    #[cfg(target_os = "windows")]
    return dirs::data_local_dir().unwrap();
    #[cfg(not(target_os = "windows"))]
    return dirs::home_dir().unwrap();
}

pub fn get_app_dir(dir: String) -> PathBuf {
    #[cfg(target_os = "windows")]
    return get_home_dir().join(dir);
    #[cfg(not(target_os = "windows"))]
    return get_home_dir().join(dir);
}

pub struct Dirs {
    pub app_dir: PathBuf,
    pub network_dir: PathBuf,
    pub db_dir: PathBuf,
}

impl Dirs {
    pub fn new(app_dir: Option<PathBuf>, network_id: NetworkId) -> Self {
        let app_dir = match app_dir {
            Some(dir) => dir,
            None => get_app_dir(String::from(".kaspalytics"))
        };

        let network_dir = app_dir.join(network_id.to_prefixed());
        let db_dir = network_dir.join("datadir");

        Self {
            app_dir,
            network_dir,
            db_dir
        }
    }
}

pub struct KaspadDirs {
    pub app_dir: PathBuf,
    pub network_dir: PathBuf,
    pub db_dir: PathBuf,
    pub utxo_index_db_dir: Option<PathBuf>,
    pub meta_db_dir: PathBuf,
    pub consensus_db_dir: PathBuf,
    pub active_consensus_db_dir: PathBuf,
}

impl KaspadDirs {
    pub fn new(app_dir: Option<PathBuf>, network_id: NetworkId) -> Self {
        let app_dir = match app_dir {
            Some(dir) => dir,
            None => get_app_dir(String::from(".rusty-kaspa"))
        };

        let network_dir = app_dir.join(network_id.to_prefixed());
        let db_dir = network_dir.join("datadir");
        let utxo_index_db_dir = if db_dir.join("utxoindex").exists() { 
            Some(db_dir.join("utxoindex"))
        } else {
            None
        };
        let meta_db_dir = db_dir.join("meta");
        let consensus_db_dir = db_dir.join("consensus");
        let active_consensus_db_dir = consensus_db_dir.join(crate::kaspad::db::get_active_consensus_dir(meta_db_dir.clone()));

        Self {
            app_dir,
            network_dir,
            db_dir,
            utxo_index_db_dir,
            meta_db_dir,
            consensus_db_dir,
            active_consensus_db_dir,
        }
    }
}
