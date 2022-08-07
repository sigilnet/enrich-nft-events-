use std::{sync::Arc, time::Duration};

use crate::configs::AppConfig;
use futures::StreamExt;
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaResult,
    message::Message,
    message::OwnedMessage,
    ClientConfig, Offset, TopicPartitionList,
};
use tokio::sync::mpsc;
use tracing::{error, warn};

pub struct StreamerMessage {
    pub message: OwnedMessage,
    consumer: Arc<StreamConsumer>,
}

impl StreamerMessage {
    pub fn commit(&self) -> KafkaResult<()> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            self.message.topic(),
            self.message.partition(),
            Offset::Offset(self.message.offset()),
        )?;
        self.consumer.commit(&tpl, CommitMode::Sync)
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
    mpsc::Receiver<StreamerMessage>,
)> {
    let (sender, receiver) = mpsc::channel(config.streamer_pool_size);

    let sender = tokio::spawn(start(
        sender,
        config.kafka_config.clone(),
        config.topics.clone(),
    ));

    Ok((sender, receiver))
}

async fn start(
    sender: mpsc::Sender<StreamerMessage>,
    kafka_config: ClientConfig,
    topics: Vec<String>,
) -> anyhow::Result<()> {
    let consumer: StreamConsumer = kafka_config.create()?;

    let topics_ref: Vec<&str> = topics.iter().map(|topic| topic.as_str()).collect();
    consumer.subscribe(&topics_ref.to_vec())?;

    let consumer_arc = Arc::new(consumer);
    let mut stream = consumer_arc.stream();

    while let Some(message_result) = stream.next().await {
        match message_result {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(borrowed_message) => {
                let message = borrowed_message.detach();
                if let Err(err) = sender
                    .send(StreamerMessage {
                        message,
                        consumer: consumer_arc.clone(),
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
