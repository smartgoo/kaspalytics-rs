use crate::config::Config as KaspalyticsConfig;
use log4rs::{
    append::console::ConsoleAppender,
    append::rolling_file::{
        policy::compound::{
            roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
        },
        RollingFileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
};

pub fn init_logger(
    config: &KaspalyticsConfig,
    log_file_base: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // let pattern = "{d(%Y-%m-%d %H:%M:%S%.3f %Z)} - {h({l})} [{M}] - {m} (({f}:{L})){n}";
    let pattern = "{d(%Y-%m-%d %H:%M:%S%.3f %Z)} - {h({l})} - {m} (({f}:{L})){n}";

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build();

    let base_path = config.kaspalytics_dirs.log_dir.join(log_file_base);
    let log_file_path = base_path.with_extension("log");
    let roll_pattern = format!("{}.{{}}.log", base_path.to_string_lossy());

    let roller = FixedWindowRoller::builder()
        .base(1)
        .build(&roll_pattern, 5)?;

    let trigger = SizeTrigger::new(10 * 1024 * 1024);

    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

    let rolling_file = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build(log_file_path.to_str().unwrap(), Box::new(policy))?;

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("file", Box::new(rolling_file)))
        .build(
            Root::builder()
                .appender("stdout")
                .appender("file")
                .build(config.log_level),
        )?;

    log4rs::init_config(config)?;

    Ok(())
}
