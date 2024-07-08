pub mod db;

use crate::args::{Args, Network};
use dirs;
use kaspa_consensus_core::network::{NetworkId, NetworkType};
use std::path::PathBuf;

const DEFAULT_DATA_DIR: &str = "datadir";

fn get_home_dir() -> PathBuf {
    #[cfg(target_os = "windows")]
    return dirs::data_local_dir().unwrap();
    #[cfg(not(target_os = "windows"))]
    return dirs::home_dir().unwrap();
}

pub fn get_app_dir() -> PathBuf {
    #[cfg(target_os = "windows")]
    return get_home_dir().join("rusty-kaspa");
    #[cfg(not(target_os = "windows"))]
    return get_home_dir().join(".rusty-kaspa");
}

pub fn network(network: Network, testnet_suffix: Option<u32>) -> NetworkId {
    match network {
        Network::Mainnet => NetworkId::new(NetworkType::Mainnet),
        Network::Testnet => NetworkId::with_suffix(NetworkType::Testnet, testnet_suffix.unwrap()),
        Network::Devnet => NetworkId::new(NetworkType::Devnet),
        Network::Simnet => NetworkId::new(NetworkType::Simnet),
    }
}

pub fn get_app_dir_from_args(args: &Args) -> PathBuf {
    let app_dir = args
        .kaspad_app_dir
        .clone()
        .unwrap_or_else(|| get_app_dir().as_path().to_str().unwrap().to_string())
        .replace('~', get_home_dir().as_path().to_str().unwrap());
    if app_dir.is_empty() {
        get_app_dir()
    } else {
        PathBuf::from(app_dir)
    }
}

pub fn get_db_dir(app_dir: PathBuf, network: NetworkId) -> PathBuf {
    app_dir.join(network.to_prefixed()).join(DEFAULT_DATA_DIR)
}