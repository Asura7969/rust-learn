use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use pin_project::pin_project;
use crate::producer::Producer;

#[pin_project]
pub struct Collector<T, P> {
    /// 跟踪状态
    status: usize,
    /// 生产数据提供者
    producer: T,
    /// 收集数据
    result: Vec<P>,
    /// 生产数据计数器
    sum: P,
    /// debugging
    num: u32,
    thread_id: Option<std::thread::ThreadId>
}


// perform the next step in polling
macro_rules! poll_step {
    ($self:ident) => {
        // no core functionality of future
        // check is working thread changed
        // can get removed
        if let Some(new_id) = check_thread_id!($self) {
            *$self.thread_id = Some(new_id);
        }
        // get new data and check if we are ready
        if new_data_and_check_ready!($self) {
            println!("MATCH {} Steps: {}", $self.num, $self.status);
            return Poll::Ready($self.result.clone()); // Return the result
        }
        return Poll::Pending // Continue polling if condition not met
    };
}


// get new data and check if read_condition is reached
macro_rules! new_data_and_check_ready {
    ($self:ident) => {{
        if !$self.producer.data_available() {
            false // Return false immediately if no data is available
        } else {
            let data = $self.producer.produce(); // Produce new data
            $self.result.push(data); // Store the produced data
            *$self.sum += data; // Update the future's sum
            // Increment future's status
            *$self.status += 1;

            // check ready-condition
            if ready_condition!($self) {
                $self.producer.stop(); // Stop if the ready condition is met
                true // Return true if the condition is met
            } else {
                false // Return false if the condition is not met
            }
        }
    }};
}


// logic to check when the collector's ready condition is met
macro_rules! ready_condition {
    ($self:ident) => {
        *$self.sum % P::from(17) == P::from(0)
    };
}


// Check if the future changes its working thread
// nothing to do with core functionality
// can be removed
macro_rules! check_thread_id {
    ($self:ident) => {{
        let new_thread_id = std::thread::current().id();
        if *$self.thread_id != Some(new_thread_id) {
            if let Some(stored_thread_id) = *$self.thread_id {
                println!(
                    "---------------->Thread ID Changed: {} from/to {:?}/{:?}",
                    $self.num, stored_thread_id, new_thread_id
                );
            }
            Some(new_thread_id) // return the updated thread ID
        } else {
            *$self.thread_id // No change, return the original thread ID
        }
    }};
}


impl <T, P> Collector<T, P>
where
    T: Producer<P>,
    P: std::convert::From<u8>
{
    const NUM_DATA: usize = 108;

    #[allow(dead_code)]
    pub fn new(producer: T, num: u32) -> Self {
        Collector {
            status: 0,
            producer,
            result: Vec::with_capacity(Collector::<T, P>::NUM_DATA),
            sum: P::from(0),
            num,
            thread_id: None
        }
    }
}

impl<T, P> Future for Collector<T, P>
where
    T: Producer<P> + std::marker::Unpin,
    P: std::convert::From<u8> + Copy + std::ops::Rem + std::ops::AddAssign,
    <P as std::ops::Rem>::Output: PartialEq<P>,
{
    type Output = Vec<P>; // The output type is a vector of produced data

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // Store the current waker
        this.producer.store_waker(cx.waker());

        match *this.status {
            // Handle case where all data is collected but no condition match
            Collector::<T, P>::NUM_DATA => {
                println!("READY, NO MATCH: {}", *this.num);
                this.producer.stop(); // Stop the producer
                Poll::Ready(this.result.clone()) // Return the result
            }
            // Handle case where collection of data not ready
            _ => {
                poll_step!(this);
            }
        }
    }
}

