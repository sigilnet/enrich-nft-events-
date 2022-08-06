use std::time::Duration;

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
};
use tracing::{debug, info, warn};

use crate::configs::AppConfig;

pub async fn ensure_topic(
    consumer: &StreamConsumer,
    admin_client: &AdminClient<DefaultClientContext>,
    config: &AppConfig,
    topic: &str,
) -> anyhow::Result<()> {
    if !config.force_create_new_topic {
        return Ok(());
    }
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(1));

    if let Err(err) = &metadata {
        warn!("Could not fetch Kafka metadata: {:?}", err);
        return Ok(());
    }

    let metadata = metadata.unwrap();

    let topic_names = metadata
        .topics()
        .iter()
        .map(|t| t.name())
        .collect::<Vec<&str>>();

    debug!("Kafka topics: {:?}", topic_names);

    let existed = metadata
        .topics()
        .iter()
        .any(|topic_metadata| topic_metadata.name() == topic);

    if !existed {
        let results = admin_client
            .create_topics(
                &[NewTopic::new(
                    topic,
                    config.new_topic_partitions,
                    TopicReplication::Fixed(config.new_topic_replication),
                )],
                &AdminOptions::new(),
            )
            .await?;

        for result in results {
            let status = result.map_err(|e| e.1);
            let status = status?;
            info!("Kafka created new topic: {:?}", status);
        }
    }

    Ok(())
}

pub async fn send_event(
    producer: &FutureProducer,
    consumer: &StreamConsumer,
    admin_client: &AdminClient<DefaultClientContext>,
    config: &AppConfig,
    topic: &str,
    key: &str,
    payload: &str,
) -> anyhow::Result<()> {
    ensure_topic(consumer, admin_client, config, topic).await?;

    let delivery_status = producer
        .send(
            FutureRecord::to(topic).payload(payload).key(key),
            Duration::from_secs(0),
        )
        .await;

    let delivery_status = delivery_status.map_err(|e| e.0);
    delivery_status?;

    Ok(())
}
