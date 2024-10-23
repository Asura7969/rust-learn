use tokio_stream::StreamExt;
use log::info;
use crate::collector::Collector;
use crate::producer::{ChannelProducer, RandProducer, TCPProducer};
use crate::stream::DataAvailable;

mod collector;
mod producer;
mod stream;

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    // let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    //
    // for i in 0..500 {
    //     let task: tokio::task::JoinHandle<()> = tokio::spawn(async move {
    //         if i % 3 == 0 {
    //             // ChannelProducer for u16
    //             let p: ChannelProducer<u16> = ChannelProducer::new();
    //             let collector = Collector::new(p, i);
    //             let _res = collector.await;
    //             //println!("Data from ChannelProducer: {:#?}", _res);
    //         } else if i % 3 == 1 {
    //             // RandProducer
    //             let p: RandProducer<i16> = RandProducer::default();
    //             let collector = Collector::new(p, i);
    //             let _res = collector.await;
    //             //println!("Data from RandProducer: {:#?}", _res);
    //         } else {
    //             // TCPProducer
    //             let addr = format!("127.0.0.1:{}", 7800 + i);
    //             let p: TCPProducer<u64> = TCPProducer::new(addr);
    //             let collector = Collector::new(p, i);
    //             let _res = collector.await;
    //             //println!("Data from TCPProducer: {:#?}", _res);
    //         }
    //     });
    //     tasks.push(task);
    // }
    // for task in tasks {
    //     let _ = task.await;
    // }
    // println!("All tasks completed");

    /// stream example
    env_logger::init();
    // Create the DataAvailable stream
    let mut data_available = DataAvailable::new();
    // Consume the stream and print the result
    let mut i = 0;
    while let Some(_data) = data_available.next().await {
        info!("Data available: {}", i);
        i += 1;
    }
}
