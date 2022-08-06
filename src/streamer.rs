use crate::configs::AppConfig;
use futures::StreamExt;
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaResult,
    message::BorrowedMessage,
};
use tokio::sync::mpsc::{self, error::SendError};
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
}

pub fn init_streamer(
    consumer: &'static StreamConsumer,
    config: AppConfig,
) -> (
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    mpsc::Receiver<StreamerMessage>,
) {
    let (sender, receiver) = mpsc::channel(config.streamer_pool_size);
    (tokio::spawn(start(consumer, sender, config)), receiver)
}

async fn start(
    consumer: &'static StreamConsumer,
    messages_sink: mpsc::Sender<StreamerMessage<'static>>,
    config: AppConfig,
) -> anyhow::Result<()> {
    let topics: Vec<&str> = config.topics.iter().map(|s| s.as_ref()).collect();

    consumer.subscribe(&topics.to_vec())?;
    let mut stream = consumer.stream();

    while let Some(message_result) = stream.next().await {
        match message_result {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(borrowed_message) => {
                if let Err(SendError(_)) = messages_sink
                    .send(StreamerMessage {
                        message: borrowed_message,
                        consumer,
                    })
                    .await
                {
                    error!(target: crate::APP, "Channel closed, exiting");
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}
