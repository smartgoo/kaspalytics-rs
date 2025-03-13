use crate::config::Config;
use chrono::Utc;
use kaspa_consensus::consensus::{
    factory::MultiConsensusManagementStore, storage::ConsensusStorage,
};
use kaspa_consensus_core::config::ConfigBuilder;
use kaspa_database::db::DB;
use std::{path::PathBuf, str::FromStr, sync::Arc};

pub const DB_MAX_FILES: i32 = 10 * 1024;

pub enum CheckpointSource {
    Consensus,
    UtxoIndex,
}

impl std::fmt::Display for CheckpointSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointSource::Consensus => write!(f, "consensus"),
            CheckpointSource::UtxoIndex => write!(f, "utxoindex"),
        }
    }
}

pub fn init_readonly_db(path: PathBuf, files_limit: i32) -> Arc<DB> {
    kaspa_database::prelude::ConnBuilder::default()
        .with_db_path(path)
        .with_files_limit(files_limit)
        .build_readonly()
        .unwrap()
}

pub fn checkpoint_path(config: Config, prefix: CheckpointSource) -> PathBuf {
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
    let mut checkpoint_path = config.checkpoint_root_dir.clone();
    checkpoint_path.push(format!("{}_{}", prefix, timestamp));
    checkpoint_path
}

pub struct UtxoIndexCheckpoint {
    pub db: Arc<DB>,
    checkpoint_path: PathBuf,
}

impl UtxoIndexCheckpoint {
    pub fn new(config: Config) -> Self {
        let db = init_readonly_db(
            config
                .kaspad_dirs
                .utxo_index_db_dir
                .as_ref()
                .unwrap()
                .to_path_buf(),
            DB_MAX_FILES,
        );

        let checkpoint_path = checkpoint_path(config, CheckpointSource::UtxoIndex);
        rocksdb::checkpoint::Checkpoint::new(&db)
            .unwrap()
            .create_checkpoint(checkpoint_path.clone())
            .unwrap();

        let checkpoint_db = init_readonly_db(
            PathBuf::from_str(checkpoint_path.to_str().unwrap()).unwrap(),
            DB_MAX_FILES,
        );

        Self {
            db: checkpoint_db,
            checkpoint_path,
        }
    }
}

impl Drop for UtxoIndexCheckpoint {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.checkpoint_path.clone()).unwrap();
    }
}

pub struct ConsensusStorageCheckpoint {
    pub inner: Arc<ConsensusStorage>,
    checkpoint_path: PathBuf,
}

impl ConsensusStorageCheckpoint {
    pub fn new(config: Config) -> Self {
        let kaspad_config: Arc<kaspa_consensus::config::Config> = Arc::new(
            ConfigBuilder::new(config.network_id.into())
                .adjust_perf_params_to_consensus_params()
                .build(),
        );

        let consensus_db = init_readonly_db(
            config.kaspad_dirs.active_consensus_db_dir.to_path_buf(),
            DB_MAX_FILES,
        );

        let checkpoint_path = checkpoint_path(config, CheckpointSource::Consensus);
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&consensus_db).unwrap();
        checkpoint
            .create_checkpoint(checkpoint_path.clone())
            .unwrap();

        let checkpoint_db = init_readonly_db(
            PathBuf::from_str(checkpoint_path.to_str().unwrap()).unwrap(),
            DB_MAX_FILES,
        );

        Self {
            inner: ConsensusStorage::new(checkpoint_db, kaspad_config),
            checkpoint_path,
        }
    }
}

impl Drop for ConsensusStorageCheckpoint {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.checkpoint_path.clone()).unwrap();
    }
}

pub fn get_active_consensus_dir(meta_db_dir: PathBuf) -> PathBuf {
    let db = init_readonly_db(meta_db_dir, 64i32);
    let store = MultiConsensusManagementStore::new(db);
    let active_consensus_dir = store.active_consensus_dir_name().unwrap().unwrap();
    PathBuf::from_str(active_consensus_dir.as_str()).unwrap()
}
