use crate::dirs::KaspalyticsDirs;
use crate::kaspad::dirs::get_app_dir;
use crate::kaspad::dirs::KaspadDirs;
use kaspa_consensus_core::network::NetworkId;
use kaspa_consensus_core::network::NetworkType;
use log::LevelFilter;
use std::env::VarError;
use std::{env, path::PathBuf, str::FromStr};
use strum_macros::{Display, EnumString};

#[derive(Clone, Copy, Display, EnumString, PartialEq)]
pub enum Env {
    #[strum(serialize = "dev")]
    Dev,

    #[strum(serialize = "uat")]
    Uat,

    #[strum(serialize = "prod")]
    Prod,
}

#[derive(Clone)]
pub struct Config {
    pub env: Env,

    pub log_level: LevelFilter,

    pub network_id: NetworkId,

    pub rpc_url: String,

    pub db_uri: String,
    pub db_max_pool_size: u32,

    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_from: String,
    pub smtp_to: String,

    pub kaspad_dirs: KaspadDirs,
    pub kaspalytics_dirs: KaspalyticsDirs,

    pub web_listen_ip: String,
    pub web_port: u16,
    pub allowed_origins: Vec<String>,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().unwrap();

        let env = Env::from_str(&env::var("ENV").unwrap()).unwrap();

        let log_level = match env::var("LOG_LEVEL") {
            Ok(v) => LevelFilter::from_str(&v).unwrap(),
            Err(VarError::NotPresent) => LevelFilter::Warn,
            Err(_) => panic!(),
        };

        let network = NetworkType::from_str(&env::var("NETWORK").unwrap()).unwrap();

        let netsuffix = env::var("NETSUFFIX")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|s| s.parse::<u32>().ok());

        let network_id = NetworkId::try_new(network)
            .unwrap_or_else(|_| NetworkId::with_suffix(network, netsuffix.unwrap()));

        let rpc_url = env::var("RPC_URL").unwrap();

        let db_uri = env::var("DB_URI").unwrap();
        let db_max_pool_size = env::var("DB_MAX_POOL_SIZE")
            .unwrap()
            .parse::<u32>()
            .unwrap_or(5);

        let smtp_host = env::var("SMTP_HOST").unwrap();
        let smtp_port = env::var("SMTP_PORT").unwrap().parse::<u16>().unwrap();
        let smtp_from = env::var("SMTP_FROM").unwrap();
        let smtp_to = env::var("SMTP_TO").unwrap();

        let kaspad_app_dir = match env::var("KASPAD_APP_DIR") {
            Ok(v) => PathBuf::from(v),
            Err(VarError::NotPresent) => get_app_dir(".rusty-kaspa".to_string()),
            Err(_) => panic!(),
        };
        let kaspad_dirs = KaspadDirs::new(kaspad_app_dir, network_id);

        let kaspalytics_app_dir = match env::var("KASPALYTICS_APP_DIR") {
            Ok(v) => PathBuf::from(v),
            Err(VarError::NotPresent) => get_app_dir(".kaspalytics".to_string()),
            Err(_) => panic!(),
        };
        let kaspalytics_dirs = KaspalyticsDirs::new(env, network_id, kaspalytics_app_dir);

        let web_listen_ip = env::var("WEB_LISTEN_IP").unwrap_or(String::from("127.0.0.1"));
        let web_port = env::var("WEB_PORT").unwrap().parse::<u16>().unwrap_or(3000);
        let allowed_origins = env::var("ALLOWED_ORIGINS")
            .unwrap()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        Config {
            env,
            log_level,
            network_id,
            rpc_url,
            db_uri,
            db_max_pool_size,
            smtp_host,
            smtp_port,
            smtp_from,
            smtp_to,
            kaspad_dirs,
            kaspalytics_dirs,
            web_listen_ip,
            web_port,
            allowed_origins,
        }
    }
}
