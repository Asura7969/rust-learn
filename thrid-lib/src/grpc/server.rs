use super::pb::MsgTime;
use crate::grpc::pb::store_service_server::{StoreService, StoreServiceServer};
use crate::grpc::pb::{Msg, MsgId};
use async_trait::async_trait;

use prost::bytes::Bytes;
use std::collections::HashMap;

use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{self};
use tokio_stream::wrappers::ReceiverStream;
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

    async fn delete(&self, request: Request<MsgId>) -> Result<Response<bool>, Status> {
        let msg = request.into_inner();
        let id = msg.id;
        match self.db.try_lock() {
            Ok(ref mut lock) => {
                lock.remove(&id);
                Ok(Response::new(true))
            }

            Err(err) => {
                event!(Level::ERROR, "Failed to acquire lock!");
                Err(Status::new(Code::FailedPrecondition, err.to_string()))
            }
        }
    }

    type subscribeStream = ReceiverStream<Result<Msg, Status>>;

    async fn subscribe(
        &self,
        request: Request<MsgId>,
    ) -> Result<Response<Self::subscribeStream>, Status> {
        let msg_id = request.into_inner();
        let id = msg_id.id;
        let msgs = match self.db.try_lock() {
            Ok(lock) => {
                let msges = lock
                    .iter()
                    .filter(|(nid, _)| **nid >= id)
                    .map(|(_, bytes)| bytes.into())
                    .collect::<Vec<Msg>>();
                Some(msges)
            }
            Err(_err) => {
                event!(Level::ERROR, "Failed to acquire lock!");
                None
            }
        };

        return_stream(msgs).await
    }

    type subscribeWithTimeStream = ReceiverStream<Result<Msg, Status>>;

    async fn subscribe_with_time(
        &self,
        request: Request<MsgTime>,
    ) -> Result<Response<Self::subscribeWithTimeStream>, Status> {
        let msg_time = request.into_inner();
        let timestamp = msg_time.timestamp;
        let msgs = match self.db.try_lock() {
            Ok(lock) => {
                let msges = lock
                    .iter()
                    .map(|(_, bytes)| bytes.into())
                    .filter(|m: &Msg| {
                        let top = m.timestamp;
                        top.is_some_and(|t| t > timestamp)
                    })
                    .collect::<Vec<Msg>>();
                Some(msges)
            }
            Err(_err) => {
                event!(Level::ERROR, "Failed to acquire lock!");
                None
            }
        };

        return_stream(msgs).await
    }
}

impl From<&Bytes> for Msg {
    fn from(bytes: &Bytes) -> Self {
        let vec = bytes.to_vec();
        let vec = vec.as_slice();
        serde_json::from_slice(vec).unwrap()
    }
}

async fn return_stream(
    msgs: Option<Vec<Msg>>,
) -> Result<Response<ReceiverStream<Result<Msg, Status>>>, Status> {
    let (tx, rx) = mpsc::channel(4);

    if let Some(messages) = msgs {
        for msg in messages {
            tx.send(Ok(msg)).await.unwrap();
        }
    } else {
        tx.send(Err(Status::new(Code::NotFound, "not found resource!")))
            .await
            .unwrap();
    }

    Ok(Response::new(ReceiverStream::new(rx)))
}
