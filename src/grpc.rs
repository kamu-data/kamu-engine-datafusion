use std::sync::Arc;

use opendatafabric::engine::grpc_generated::engine_server::Engine as EngineGRPC;
use opendatafabric::engine::grpc_generated::{
    ExecuteQueryRequest as ExecuteQueryRequestGRPC,
    ExecuteQueryResponse as ExecuteQueryResponseGRPC,
};
use opendatafabric::serde::flatbuffers::FlatbuffersEngineProtocol;
use opendatafabric::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::engine::Engine;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineGRPCImpl {
    engine: Arc<Engine>,
}

impl EngineGRPCImpl {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tonic::async_trait]
impl EngineGRPC for EngineGRPCImpl {
    type ExecuteQueryStream = ReceiverStream<Result<ExecuteQueryResponseGRPC, Status>>;

    async fn execute_query(
        &self,
        request_grpc: Request<ExecuteQueryRequestGRPC>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let span = tracing::span!(tracing::Level::INFO, "execute_query");
        let _enter = span.enter();

        let request = FlatbuffersEngineProtocol
            .read_execute_query_request(&request_grpc.get_ref().flatbuffer)
            .unwrap();

        info!(message = "Got request", request = ?request);

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let engine = self.engine.clone();

        tokio::spawn(async move {
            let response = engine.execute_query(request).await.unwrap();

            let response_fb = FlatbuffersEngineProtocol
                .write_execute_query_response(&response)
                .unwrap();

            let response_grpc = ExecuteQueryResponseGRPC {
                flatbuffer: response_fb.collapse_vec(),
            };

            tx.send(Ok(response_grpc)).await.unwrap();

            // let responses = [
            //     ExecuteQueryResponse::Progress,
            //     ExecuteQueryResponse::Progress,
            //     ExecuteQueryResponse::Error,
            // ];

            // for response in responses {
            //     tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            //     let response_fb = FlatbuffersEngineProtocol
            //         .write_execute_query_response(&response)
            //         .unwrap();

            //     let response_grpc = ExecuteQueryResponseGRPC {
            //         flatbuffer: response_fb.collapse_vec(),
            //     };

            //     tx.send(Ok(response_grpc)).await.unwrap();
            // }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
