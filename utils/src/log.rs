use crate::config::Config as KaspalyticsConfig;
use log4rs::{
    append::console::ConsoleAppender,
    append::rolling_file::{
        policy::compound::{
            roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
        },
        RollingFileAppender,
    },
    config::{Appender, Config, Logger, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

#[derive(Debug, Clone, Copy)]
pub enum LogTarget {
    Cli,
    Daemon,
    Web,
    WebErr,
}

impl LogTarget {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogTarget::Cli => "cli",
            LogTarget::Daemon => "daemon",
            LogTarget::Web => "web",
            LogTarget::WebErr => "web_err",
        }
    }
}

fn create_stdout_appender() -> ConsoleAppender {
    let pattern = "{d(%Y-%m-%d %H:%M:%S%.3f %Z)} [{h({l})}] - {m} (({f}:{L})){n}";
    ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build()
}

fn create_rolling_file_appender(
    base_path: &std::path::Path,
    pattern: &str,
) -> Result<RollingFileAppender, Box<dyn std::error::Error>> {
    let log_file_path = base_path.with_extension("log");
    let roll_pattern = format!("{}.{{}}.log", base_path.to_string_lossy());

    let roller = FixedWindowRoller::builder()
        .base(1)
        .build(&roll_pattern, 5)?;

    let trigger = SizeTrigger::new(10 * 1024 * 1024);
    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

    RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build(log_file_path.to_str().unwrap(), Box::new(policy))
        .map_err(|e| e.into())
}

fn setup_daemon_logger(
    config: &KaspalyticsConfig,
    log_file_base: &str,
    pattern: &str,
) -> Result<Appender, Box<dyn std::error::Error>> {
    let base_path = config.kaspalytics_dirs.log_dir.join(log_file_base);
    let rolling_file = create_rolling_file_appender(&base_path, pattern)?;

    Ok(Appender::builder()
        .filter(Box::new(ThresholdFilter::new(log::LevelFilter::Info)))
        .build("daemon", Box::new(rolling_file)))
}

fn setup_web_loggers(
    config: &KaspalyticsConfig,
    pattern: &str,
) -> Result<(Appender, Appender), Box<dyn std::error::Error>> {
    let web_base_path = config.kaspalytics_dirs.log_dir.join("web");
    let web_err_path = web_base_path.with_extension("err.log");

    let web_rolling_file = create_rolling_file_appender(&web_base_path, pattern)?;
    let web_err_rolling_file = create_rolling_file_appender(&web_err_path, pattern)?;

    let web_appender = Appender::builder()
        .filter(Box::new(ThresholdFilter::new(log::LevelFilter::Info)))
        .build("web", Box::new(web_rolling_file));

    let web_err_appender = Appender::builder()
        .filter(Box::new(ThresholdFilter::new(log::LevelFilter::Error)))
        .build("web_err", Box::new(web_err_rolling_file));

    Ok((web_appender, web_err_appender))
}

fn setup_cli_logger(
    config: &KaspalyticsConfig,
    pattern: &str,
) -> Result<Appender, Box<dyn std::error::Error>> {
    let cli_base_path = config.kaspalytics_dirs.log_dir.join("cli");
    let cli_rolling_file = create_rolling_file_appender(&cli_base_path, pattern)?;

    Ok(Appender::builder()
        .filter(Box::new(ThresholdFilter::new(log::LevelFilter::Info)))
        .build("cli", Box::new(cli_rolling_file)))
}

pub fn init_logger(
    config: &KaspalyticsConfig,
    log_file_base: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let pattern = "{d(%Y-%m-%d %H:%M:%S%.3f %Z)} - {h({l})} - {m} (({f}:{L})){n}";

    std::fs::create_dir_all(config.kaspalytics_dirs.log_dir.as_path())?;

    let stdout = create_stdout_appender();
    let daemon = setup_daemon_logger(config, log_file_base, pattern)?;
    let (web, web_err) = setup_web_loggers(config, pattern)?;
    let cli = setup_cli_logger(config, pattern)?;

    let web_logger = Logger::builder()
        .appender("web")
        .additive(false)
        .build("web", config.log_level);

    let web_err_logger = Logger::builder()
        .appender("web_err")
        .additive(false)
        .build("web_err", config.log_level);

    let daemon_logger = Logger::builder()
        .appender("daemon")
        .appender("stdout")
        .additive(false)
        .build("daemon", config.log_level);

    let cli_logger = Logger::builder()
        .appender("cli")
        .appender("stdout")
        .additive(false)
        .build("cli", config.log_level);

    let tower_http_logger = Logger::builder().build("tower_http", log::LevelFilter::Warn);

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(daemon)
        .appender(web)
        .appender(web_err)
        .appender(cli)
        .logger(web_logger)
        .logger(web_err_logger)
        .logger(daemon_logger)
        .logger(cli_logger)
        .logger(tower_http_logger)
        .build(Root::builder().build(config.log_level))?;

    log4rs::init_config(config)?;

    Ok(())
}
