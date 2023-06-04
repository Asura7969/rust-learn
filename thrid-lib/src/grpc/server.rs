use super::pb::MsgTime;
use crate::grpc::pb::store_service_server::{StoreService, StoreServiceServer};
use crate::grpc::pb::{Msg, MsgId};
use async_trait::async_trait;
use futures::Stream;
use prost::bytes::Bytes;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tracing::{event, Level};
use tracing_subscriber;

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:3000".parse().unwrap();

    let store: KvStoreService = KvStoreService::default();
    let kv_service = StoreServiceServer::new(store);

    println!("StoreServiceServer listening on {}", addr);

    Server::builder()
        // GrpcWeb is over http1 so we must enable it.
        .accept_http1(true)
        .add_service(kv_service)
        .serve(addr)
        .await?;

    Ok(())
}

type State = Arc<Mutex<HashMap<i64, Bytes>>>;

struct KvStoreService {
    db: State,
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<Msg, Status>> + Send>>;

impl Default for KvStoreService {
    fn default() -> KvStoreService {
        let db = Arc::new(Mutex::new(HashMap::default()));
        Self { db }
    }
}

#[async_trait]
impl StoreService for KvStoreService
// where
//     K: Send + Sync + std::hash::Hash + std::cmp::Eq + std::cmp::PartialEq + 'static,
//     V: Send + Sync + 'static,
{
    async fn get(&self, request: Request<MsgId>) -> Result<Response<Msg>, Status> {
        let msg_id = request.into_inner();
        match self.db.try_lock() {
            Ok(lock) => {
                if let Some(bytes) = lock.get(&msg_id.id) {
                    let vec = bytes.to_vec();
                    let vec = vec.as_slice();
                    let deserialized: Msg = serde_json::from_slice(vec).unwrap();
                    Ok(Response::new(deserialized))
                } else {
                    Err(Status::new(Code::NotFound, ""))
                }
            }
            Err(err) => {
                event!(Level::ERROR, "Failed to acquire lock!");
                Err(Status::new(Code::FailedPrecondition, err.to_string()))
            }
        }
    }
    async fn send(&self, request: Request<Msg>) -> Result<Response<bool>, Status> {
        let msg = request.into_inner();
        let id = msg.id;

        let serialized = serde_json::to_vec(&msg).unwrap();

        match self.db.try_lock() {
            Ok(ref mut lock) => {
                lock.insert(id, Bytes::from(serialized));
                Ok(Response::new(true))
            }

            Err(err) => {
                event!(Level::ERROR, "Failed to acquire lock!");
                Err(Status::new(Code::FailedPrecondition, err.to_string()))
            }
        }
    }

    async fn delete(&self, _request: Request<MsgId>) -> Result<Response<bool>, Status> {
        todo!()
    }

    type subscribeStream = ResponseStream;

    async fn subscribe(
        &self,
        _request: Request<MsgId>,
    ) -> Result<Response<Self::subscribeStream>, Status> {
        todo!()
    }

    type subscribeWithTimeStream = ResponseStream;

    async fn subscribe_with_time(
        &self,
        _request: Request<MsgTime>,
    ) -> Result<Response<Self::subscribeWithTimeStream>, Status> {
        todo!()
    }
}
