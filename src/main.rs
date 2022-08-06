use std::path::PathBuf;

use clap::Parser;
use configs::{AppConfig, Opts};
use futures::StreamExt;
use openssl_probe::init_ssl_cert_env_vars;
use rdkafka::message::Message;
use streamer::init_streamer;
use streamer::StreamerMessage;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::FmtSubscriber;

mod configs;
mod streamer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_ssl_cert_env_vars();

    let opts: Opts = Opts::parse();
    let config = AppConfig::new(PathBuf::from(opts.home_dir))?;

    init_tracer(&config);

    let (sender, stream) = init_streamer(config)?;
    info!("Start streamer...");

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(handle_message)
        .buffer_unordered(usize::from(opts.concurrency.get()));

    while let Some(handle_message) = handlers.next().await {
        handle_message?;
    }

    drop(handlers);

    match sender.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::Error::from(e)), // JoinError
    }
}

fn init_tracer(config: &AppConfig) {
    let mut env_filter = EnvFilter::new(&config.log_level);

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    FmtSubscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}

async fn handle_message(streamer_message: StreamerMessage<'static>) -> anyhow::Result<()> {
    debug!(
        "Received kafka message: {:?}",
        streamer_message.message.offset()
    );
    streamer_message.commit()?;
    Ok(())
}
