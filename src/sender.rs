use std::time::Duration;

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    producer::{FutureProducer, FutureRecord},
};
use tracing::{debug, info, warn};

use crate::{configs::AppConfig, streamer::StreamerMessage};

pub async fn ensure_topic(
    admin_client: &AdminClient<DefaultClientContext>,
    streamer_message: &StreamerMessage,
    config: &AppConfig,
    topic: &str,
) -> anyhow::Result<()> {
    if !config.force_create_new_topic {
        return Ok(());
    }
    let topics = streamer_message.fetch_all_topics();

    if let Err(err) = &topics {
        warn!("Could not fetch Kafka topics: {:?}", err);
        return Ok(());
    }

    let topic_names = topics.unwrap();

    debug!("Kafka topics: {:?}", &topic_names);

    let existed = topic_names.iter().any(|topic_name| topic_name == topic);

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
    admin_client: &AdminClient<DefaultClientContext>,
    streamer_message: &StreamerMessage,
    config: &AppConfig,
    topic: &str,
    key: &str,
    payload: &str,
) -> anyhow::Result<()> {
    ensure_topic(admin_client, streamer_message, config, topic).await?;

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
