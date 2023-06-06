use crate::grpc::pb::store_service_client::StoreServiceClient;

use super::pb::{MsgBuilder, MsgIdBuilder, MsgTimeBuilder};

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StoreServiceClient::connect("http://[::1]:3000").await?;

    let msg = MsgBuilder::default()
        .id(1_i64)
        .data(vec![0, 2, 4, 6])
        .timestamp(1686036856542_i64)
        .private_build()
        .unwrap();

    let _r = client.send(msg).await?;

    let msg_id = MsgIdBuilder::default().id(1_i64).private_build().unwrap();
    let c_msg_id = msg_id.clone();
    let _r = client.get(msg_id).await?;

    let _r = client.subscribe(c_msg_id).await?;

    let msg_time = MsgTimeBuilder::default()
        .timestamp(1686036856542_i64)
        .private_build()
        .unwrap();

    let _r = client.subscribe_with_time(msg_time).await?;

    Ok(())
}
