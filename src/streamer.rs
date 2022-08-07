use std::time::Duration;

use crate::configs::AppConfig;
use futures::StreamExt;
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaResult,
    message::BorrowedMessage,
};
use tokio::sync::mpsc;
use tracing::{error, warn};

pub struct StreamerMessage<'a> {
    pub message: BorrowedMessage<'a>,
    consumer: &'a StreamConsumer,
}

impl<'a> StreamerMessage<'a> {
    pub fn commit(&self) -> KafkaResult<()> {
        self.consumer
            .commit_message(&self.message, CommitMode::Sync)
    }

    pub fn fetch_all_topics(&self) -> anyhow::Result<Vec<String>> {
        let metadata = self.consumer.fetch_metadata(None, Duration::from_secs(1))?;

        let topics: Vec<String> = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .collect();

        Ok(topics)
    }
}

#[allow(clippy::type_complexity)]
pub fn init_streamer(
    config: &AppConfig,
) -> anyhow::Result<(
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    mpsc::Receiver<StreamerMessage<'static>>,
)> {
    //TODO: refactor this
    let consumer: StreamConsumer = config.kafka_config.create()?;
    let consumer_box = Box::new(consumer);
    let consumer_ref: &'static mut StreamConsumer = Box::leak(consumer_box);

    let (sender, receiver) = mpsc::channel(config.streamer_pool_size);

    let sender = tokio::spawn(start(consumer_ref, sender, config.topics.clone()));

    Ok((sender, receiver))
}

async fn start(
    consumer: &'static StreamConsumer,
    sender: mpsc::Sender<StreamerMessage<'static>>,
    topics: Vec<String>,
) -> anyhow::Result<()> {
    let topics_ref: Vec<&str> = topics.iter().map(|topic| topic.as_str()).collect();
    consumer.subscribe(&topics_ref.to_vec())?;
    let mut stream = consumer.stream();

    while let Some(message_result) = stream.next().await {
        match message_result {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(borrowed_message) => {
                if let Err(err) = sender
                    .send(StreamerMessage {
                        message: borrowed_message,
                        consumer,
                    })
                    .await
                {
                    error!("Channel closed, exiting with error {}", err);
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}
