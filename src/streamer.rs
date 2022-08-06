use crate::configs::AppConfig;
use futures::StreamExt;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::OwnedMessage,
};
use tokio::sync::mpsc::{self, error::SendError};
use tracing::{error, warn};

pub async fn start(
    messages_sink: mpsc::Sender<OwnedMessage>,
    config: AppConfig,
) -> anyhow::Result<()> {
    let topics: Vec<&str> = config.topics.iter().map(|s| s.as_ref()).collect();
    let consumer: StreamConsumer = config.kafka_config.create()?;

    consumer.subscribe(&topics.to_vec())?;
    let mut stream = consumer.stream();

    while let Some(message_result) = stream.next().await {
        match message_result {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(borrowed_message) => {
                let message = borrowed_message.detach();
                if let Err(SendError(_)) = messages_sink.send(message).await {
                    error!(target: crate::APP, "Channel closed, exiting");
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}
