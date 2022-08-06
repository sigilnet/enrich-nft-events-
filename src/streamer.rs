use crate::{configs::AppConfig, sender::send_event};
use futures::StreamExt;
use rdkafka::{
    admin::AdminClient,
    client::DefaultClientContext,
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaResult,
    message::BorrowedMessage,
    producer::FutureProducer,
};
use tokio::sync::mpsc::{self, error::SendError};
use tracing::{error, warn};

pub struct StreamerMessage<'a> {
    pub message: BorrowedMessage<'a>,
    consumer: &'a StreamConsumer,
    producer: &'a FutureProducer,
    admin_client: &'a AdminClient<DefaultClientContext>,
}

impl<'a> StreamerMessage<'a> {
    pub fn commit(&self) -> KafkaResult<()> {
        self.consumer
            .commit_message(&self.message, CommitMode::Sync)
    }

    pub async fn send(
        &self,
        config: &AppConfig,
        topic: &str,
        key: &str,
        payload: &str,
    ) -> anyhow::Result<()> {
        send_event(
            self.producer,
            self.consumer,
            self.admin_client,
            config,
            topic,
            key,
            payload,
        )
        .await
    }
}

#[allow(clippy::type_complexity)]
pub fn init_streamer(
    config: &'static AppConfig,
) -> anyhow::Result<(
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    mpsc::Receiver<StreamerMessage<'static>>,
)> {
    let consumer: StreamConsumer = config.kafka_config.create()?;
    let consumer_box = Box::new(consumer);
    let consumer_ref: &'static mut StreamConsumer = Box::leak(consumer_box);

    let producer: FutureProducer = config.kafka_config.create()?;
    let producer_box = Box::new(producer);
    let producer_ref: &'static mut FutureProducer = Box::leak(producer_box);

    let admin_client: AdminClient<DefaultClientContext> = config.kafka_config.create()?;
    let admin_client_box = Box::new(admin_client);
    let admin_client_ref: &'static mut AdminClient<DefaultClientContext> =
        Box::leak(admin_client_box);

    let (sender, receiver) = mpsc::channel(config.streamer_pool_size);

    let sender = tokio::spawn(start(
        consumer_ref,
        producer_ref,
        admin_client_ref,
        sender,
        config,
    ));

    Ok((sender, receiver))
}

async fn start(
    consumer: &'static StreamConsumer,
    producer: &'static FutureProducer,
    admin_client: &'static AdminClient<DefaultClientContext>,
    sender: mpsc::Sender<StreamerMessage<'static>>,
    config: &AppConfig,
) -> anyhow::Result<()> {
    let topics: Vec<&str> = config.topics.iter().map(|s| s.as_ref()).collect();

    consumer.subscribe(&topics.to_vec())?;
    let mut stream = consumer.stream();

    while let Some(message_result) = stream.next().await {
        match message_result {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(borrowed_message) => {
                if let Err(SendError(_)) = sender
                    .send(StreamerMessage {
                        message: borrowed_message,
                        consumer,
                        producer,
                        admin_client,
                    })
                    .await
                {
                    error!("Channel closed, exiting");
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}
