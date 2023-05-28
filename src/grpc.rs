use std::backtrace::{Backtrace, BacktraceStatus};
use std::error::Error;
use std::sync::Arc;

use internal_error::InternalError;
use opendatafabric::engine::grpc_generated::engine_server::Engine as EngineGRPC;
use opendatafabric::engine::grpc_generated::{
    ExecuteQueryRequest as ExecuteQueryRequestGRPC,
    ExecuteQueryResponse as ExecuteQueryResponseGRPC,
};
use opendatafabric::engine::ExecuteQueryError;
use opendatafabric::serde::flatbuffers::FlatbuffersEngineProtocol;
use opendatafabric::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use opendatafabric::{ExecuteQueryResponse, ExecuteQueryResponseInternalError};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::engine::Engine;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineGRPCImpl {
    engine: Arc<Engine>,
}

impl EngineGRPCImpl {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    fn into_serializable_error(err: InternalError) -> ExecuteQueryResponseInternalError {
        use std::fmt::Write;

        let mut current_err: &dyn Error = &err;

        let mut message = String::with_capacity(200);
        write!(message, "{}", current_err).unwrap();

        let mut backtrace = current_err.request_ref::<Backtrace>();

        while let Some(source) = current_err.source() {
            current_err = source;

            // Chain error messages
            write!(message, ": {}", current_err).unwrap();

            // Find inner-most backtrace
            if let Some(bt) = current_err.request_ref::<Backtrace>() {
                if bt.status() == BacktraceStatus::Captured {
                    backtrace = Some(bt);
                }
            }
        }

        let backtrace = match backtrace {
            Some(bt) if bt.status() == BacktraceStatus::Captured => Some(format!("{}", bt)),
            _ => None,
        };

        ExecuteQueryResponseInternalError { message, backtrace }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tonic::async_trait]
impl EngineGRPC for EngineGRPCImpl {
    type ExecuteQueryStream = ReceiverStream<Result<ExecuteQueryResponseGRPC, Status>>;

    #[tracing::instrument(level = "info", skip_all)]
    async fn execute_query(
        &self,
        request_grpc: Request<ExecuteQueryRequestGRPC>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let request = FlatbuffersEngineProtocol
            .read_execute_query_request(&request_grpc.get_ref().flatbuffer)
            .unwrap();

        tracing::info!(?request, "Got request");

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let engine = self.engine.clone();

        tokio::spawn(async move {
            let response = match engine.execute_query(request).await {
                Ok(res) => ExecuteQueryResponse::Success(res),
                Err(ExecuteQueryError::InvalidQuery(err)) => {
                    ExecuteQueryResponse::InvalidQuery(err)
                }
                Err(ExecuteQueryError::EngineInternalError(err)) => {
                    ExecuteQueryResponse::InternalError(err)
                }
                Err(ExecuteQueryError::InternalError(err)) => {
                    ExecuteQueryResponse::InternalError(Self::into_serializable_error(err))
                }
            };

            tracing::info!(?response, "Produced response");

            let response_fb = FlatbuffersEngineProtocol
                .write_execute_query_response(&response)
                .unwrap();

            let response_grpc = ExecuteQueryResponseGRPC {
                flatbuffer: response_fb.collapse_vec(),
            };

            tx.send(Ok(response_grpc)).await.unwrap();
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
