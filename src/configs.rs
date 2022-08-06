use std::collections::HashMap;

use clap::Parser;
use rdkafka::ClientConfig;
use serde::Deserialize;

pub const CONFIG_FILENAME: &str = "config.toml";

#[derive(Parser, Debug)]
#[clap(version = "0.1", author = "Sigil Network <contact@sigilnet.com>")]
pub(crate) struct Opts {
    #[clap(short, long, default_value = ".")]
    pub home_dir: String,
    #[clap(long, default_value = "1")]
    pub concurrency: std::num::NonZeroU16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub kafka: HashMap<String, String>,

    #[serde(skip)]
    pub kafka_config: ClientConfig,

    pub topics: Vec<String>,

    pub streamer_pool_size: usize,

    pub log_level: String,

    pub near_node_url: String,

    pub topic_output_suffix: String,

    pub force_create_new_topic: bool,

    pub new_topic_partitions: i32,

    pub new_topic_replication: i32,
}

impl AppConfig {
    pub fn new(home_dir: std::path::PathBuf) -> anyhow::Result<Self> {
        let conf_file = home_dir.join(CONFIG_FILENAME);
        let conf = config::Config::builder()
            .add_source(config::File::from(conf_file))
            .build()?;

        let mut nes_conf = conf.try_deserialize::<Self>()?;
        nes_conf.init_kafka_config();

        Ok(nes_conf)
    }

    fn init_kafka_config(&mut self) {
        let mut kafka_conf = ClientConfig::new();
        self.kafka.iter().for_each(|(k, v)| {
            kafka_conf.set(k, v);
        });
        self.kafka_config = kafka_conf;
    }
}
