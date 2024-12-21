use kaspa_consensus::consensus::{
    factory::MultiConsensusManagementStore, storage::ConsensusStorage,
};
use kaspa_consensus_core::{config::ConfigBuilder, network::NetworkId};
use std::{path::{Path, PathBuf}, str::FromStr, sync::Arc};

pub fn get_active_consensus_dir(meta_db_dir: PathBuf) -> PathBuf {
    let db = kaspa_database::prelude::ConnBuilder::default()
        .with_db_path(meta_db_dir)
        .with_files_limit(128)
        .build_readonly()
        .unwrap();
    let store = MultiConsensusManagementStore::new(db);
    let active_consensus_dir = store.active_consensus_dir_name().unwrap().unwrap();
    PathBuf::from_str(active_consensus_dir.as_str()).unwrap()
}

pub fn init_consensus_storage(
    network: NetworkId,
    active_consensus_db_dir: &Path,
) -> Arc<ConsensusStorage> {
    let config: Arc<kaspa_consensus::config::Config> = Arc::new(
        ConfigBuilder::new(network.into())
            .adjust_perf_params_to_consensus_params()
            .build(),
    );

    let db = kaspa_database::prelude::ConnBuilder::default()
        .with_db_path(active_consensus_db_dir.to_path_buf())
        .with_files_limit(64) // TODO files limit?
        .build_readonly()
        .unwrap();

    ConsensusStorage::new(db, config)
}
