use crossbeam_epoch::{self as epoch, Atomic, Owned};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use std::sync::atomic::Ordering;


struct MutexQueue<T> {
    queue: Mutex<Vec<T>>,
}
impl<T> MutexQueue<T> {
    fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
        }
    }
    fn enqueue(&self, data: T) {
        let mut queue = self.queue.lock().unwrap();
        queue.push(data);
    }
    fn dequeue(&self) -> Option<T> {
        let mut queue = self.queue.lock().unwrap();
        if !queue.is_empty() {
            Some(queue.remove(0))
        } else {
            None
        }
    }
}
use crossbeam_queue::ArrayQueue;
pub fn benchmark_free_queue() {
    const NUM_THREADS: usize = 4;
    const NUM_OPERATIONS: usize = 1000000;
    // Lock-free queue benchmark
    // let lock_free_queue = Arc::new(LockFreeQueue::new());

    let lock_free_queue = Arc::new(ArrayQueue::new(NUM_THREADS));
    let start = Instant::now();
    let mut handles = vec![];
    for _ in 0..NUM_THREADS {

        let queue = Arc::clone(&lock_free_queue);
        handles.push(thread::spawn(move || {
            for i in 0..NUM_OPERATIONS {
                let _ = queue.push(i);
                queue.pop();
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let duration = start.elapsed();
    println!("Lock-Free Queue Time: {:?}", duration);
    // Mutex-based queue benchmark
    let mutex_queue = Arc::new(MutexQueue::new());
    let start = Instant::now();
    let mut handles = vec![];
    for _ in 0..NUM_THREADS {
        let queue = Arc::clone(&mutex_queue);
        handles.push(thread::spawn(move || {
            for i in 0..NUM_OPERATIONS {
                queue.enqueue(i);
                queue.dequeue();
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let duration = start.elapsed();
    println!("Mutex Queue Time: {:?}", duration);
}
