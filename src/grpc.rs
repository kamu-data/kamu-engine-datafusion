use std::backtrace::{Backtrace, BacktraceStatus};
use std::error::Error;
use std::sync::Arc;

use internal_error::InternalError;
use opendatafabric::engine::grpc_generated::engine_server::Engine as EngineGRPC;
use opendatafabric::engine::grpc_generated::{
    RawQueryRequest as RawQueryRequestGRPC,
    RawQueryResponse as RawQueryResponseGRPC,
    TransformRequest as TransformRequestGRPC,
    TransformResponse as TransformResponseGRPC,
};
use opendatafabric::engine::{ExecuteRawQueryError, ExecuteTransformError};
use opendatafabric::serde::flatbuffers::FlatbuffersEngineProtocol;
use opendatafabric::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use opendatafabric::{
    RawQueryResponse,
    RawQueryResponseInternalError,
    TransformResponse,
    TransformResponseInternalError,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::Instrument;

use crate::engine::Engine;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineGRPCImpl {
    engine: Arc<Engine>,
}

impl EngineGRPCImpl {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    fn format_error_message_and_backtrace(err: InternalError) -> (String, Option<String>) {
        use std::fmt::Write;

        let mut current_err: &dyn Error = &err;

        let mut message = String::with_capacity(200);
        write!(message, "{}", current_err).unwrap();

        let mut backtrace = core::error::request_ref::<Backtrace>(current_err);

        while let Some(source) = current_err.source() {
            current_err = source;

            // Chain error messages
            write!(message, ": {}", current_err).unwrap();

            // Find inner-most backtrace
            if let Some(bt) = core::error::request_ref::<Backtrace>(current_err) {
                if bt.status() == BacktraceStatus::Captured {
                    backtrace = Some(bt);
                }
            }
        }

        let backtrace = match backtrace {
            Some(bt) if bt.status() == BacktraceStatus::Captured => Some(format!("{}", bt)),
            _ => None,
        };

        (message, backtrace)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tonic::async_trait]
impl EngineGRPC for EngineGRPCImpl {
    type ExecuteRawQueryStream = ReceiverStream<Result<RawQueryResponseGRPC, Status>>;
    type ExecuteTransformStream = ReceiverStream<Result<TransformResponseGRPC, Status>>;

    #[tracing::instrument(level = "info", skip_all)]
    async fn execute_raw_query(
        &self,
        request_grpc: Request<RawQueryRequestGRPC>,
    ) -> Result<Response<Self::ExecuteRawQueryStream>, Status> {
        let request = FlatbuffersEngineProtocol
            .read_raw_query_request(&request_grpc.get_ref().flatbuffer)
            .unwrap();

        tracing::info!(?request, "Got request");

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let engine = self.engine.clone();

        tokio::spawn(
            async move {
                let response = match engine.execute_raw_query(request).await {
                    Ok(res) => RawQueryResponse::Success(res),
                    Err(ExecuteRawQueryError::InvalidQuery(err)) => {
                        RawQueryResponse::InvalidQuery(err)
                    }
                    Err(ExecuteRawQueryError::EngineInternalError(err)) => {
                        RawQueryResponse::InternalError(err)
                    }
                    Err(ExecuteRawQueryError::InternalError(err)) => {
                        let (message, backtrace) = Self::format_error_message_and_backtrace(err);
                        RawQueryResponse::InternalError(RawQueryResponseInternalError {
                            message,
                            backtrace,
                        })
                    }
                };

                tracing::info!(?response, "Produced response");

                let response_fb = FlatbuffersEngineProtocol
                    .write_raw_query_response(&response)
                    .unwrap();

                let response_grpc = RawQueryResponseGRPC {
                    flatbuffer: response_fb.collapse_vec(),
                };

                tx.send(Ok(response_grpc)).await.unwrap();
            }
            .instrument(tracing::Span::current()),
        );

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn execute_transform(
        &self,
        request_grpc: Request<TransformRequestGRPC>,
    ) -> Result<Response<Self::ExecuteTransformStream>, Status> {
        let request = FlatbuffersEngineProtocol
            .read_transform_request(&request_grpc.get_ref().flatbuffer)
            .unwrap();

        tracing::info!(?request, "Got request");

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let engine = self.engine.clone();

        tokio::spawn(
            async move {
                let response = match engine.execute_transform(request).await {
                    Ok(res) => TransformResponse::Success(res),
                    Err(ExecuteTransformError::InvalidQuery(err)) => {
                        TransformResponse::InvalidQuery(err)
                    }
                    Err(ExecuteTransformError::EngineInternalError(err)) => {
                        TransformResponse::InternalError(err)
                    }
                    Err(ExecuteTransformError::InternalError(err)) => {
                        let (message, backtrace) = Self::format_error_message_and_backtrace(err);
                        TransformResponse::InternalError(TransformResponseInternalError {
                            message,
                            backtrace,
                        })
                    }
                };

                tracing::info!(?response, "Produced response");

                let response_fb = FlatbuffersEngineProtocol
                    .write_transform_response(&response)
                    .unwrap();

                let response_grpc = TransformResponseGRPC {
                    flatbuffer: response_fb.collapse_vec(),
                };

                tx.send(Ok(response_grpc)).await.unwrap();
            }
            .instrument(tracing::Span::current()),
        );

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
