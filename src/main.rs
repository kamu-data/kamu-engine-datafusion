use std::sync::Arc;

use kamu_engine_datafusion::engine::Engine;
use kamu_engine_datafusion::grpc::EngineGRPCImpl;
use opendatafabric::engine::grpc_generated::engine_server::EngineServer;
use tonic::transport::Server;

/////////////////////////////////////////////////////////////////////////////////////////

const BINARY_NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_LOGGING_CONFIG: &str = "info";

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let addr = "0.0.0.0:2884";

    tracing::info!(
        message = "Starting DataFusion ODF engine",
        version = VERSION,
        addr = addr,
    );

    let addr = addr.parse()?;
    let engine = Arc::new(Engine::new().await);
    let engine_grpc = EngineGRPCImpl::new(engine);

    Server::builder()
        .add_service(EngineServer::new(engine_grpc))
        .serve(addr)
        .await?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////

fn init_logging() {
    use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
    use tracing_log::LogTracer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{EnvFilter, Registry};

    // Redirect all standard logging to tracing events
    LogTracer::init().expect("Failed to set LogTracer");

    // Use configuration from RUST_LOG env var if provided
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new(DEFAULT_LOGGING_CONFIG));

    // TODO: Switch to standard tracing format
    let formatting_layer = BunyanFormattingLayer::new(BINARY_NAME.to_owned(), std::io::stdout);
    let subscriber = Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
}
