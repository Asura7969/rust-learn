#![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
use crossbeam::epoch;
use epoch::{Atomic, Owned, Shared};
/// https://clslaid.icu/implement-lockless-unsafe-queue/#%E5%AE%8C%E5%85%A8%E4%BB%A3%E7%A0%81%E4%B8%8E%E6%8E%A8%E8%8D%90%E9%98%85%E8%AF%BB
use std::sync::atomic::{AtomicUsize, Ordering};

type NodePtr<T> = Atomic<Node<T>>;

struct Node<T> {
    pub item: Option<T>,
    pub next: NodePtr<T>,
}

impl<T> Node<T> {
    pub fn new(x: T) -> Self {
        Self {
            item: Some(x),
            next: Atomic::null(),
        }
    }

    pub fn new_empty() -> Self {
        Self {
            item: None,
            next: Atomic::null(),
        }
    }
}

pub struct LinkedQueue<T> {
    len: AtomicUsize,
    head: NodePtr<T>,
    tail: NodePtr<T>,
}

impl<T> LinkedQueue<T> {
    pub fn new() -> Self {
        let head = Atomic::from(Node::new_empty());
        let tail = head.clone();
        Self {
            len: AtomicUsize::new(0),
            head,
            tail,
        }
    }

    pub fn size(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        0 == self.len.load(Ordering::SeqCst)
    }

    pub fn push(&mut self, item: T) {
        let guard = epoch::pin();

        let new_node = Owned::new(Node::new(item)).into_shared(&guard);
        let old_tail = self.tail.load(Ordering::Acquire, &guard);
        unsafe {
            let mut tail_next = &(*old_tail.as_raw()).next;
            while tail_next
                .compare_exchange(
                    Shared::null(),
                    new_node,
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                )
                .is_err()
            {
                let mut tail = tail_next.load(Ordering::Acquire, &guard).as_raw();

                loop {
                    let nxt = (*tail).next.load(Ordering::Acquire, &guard);
                    if nxt.is_null() {
                        break;
                    }
                    tail = nxt.as_raw();
                }

                tail_next = &(*tail).next;
            }
        }

        let _ = self.tail.compare_exchange(
            old_tail,
            new_node,
            Ordering::Release,
            Ordering::Relaxed,
            &guard,
        );
        self.len.fetch_add(1, Ordering::SeqCst);
    }

    pub fn pop(&self) -> Option<T> {
        let mut data = None;
        if self.is_empty() {
            return data;
        }
        let guard = &epoch::pin();
        unsafe {
            loop {
                let head = self.head.load(Ordering::Acquire, guard);
                let mut next = (*head.as_raw()).next.load(Ordering::Acquire, guard);
                if next.is_null() {
                    return None;
                }

                if self
                    .head
                    .compare_exchange(head, next, Ordering::Release, Ordering::Relaxed, guard)
                    .is_ok()
                {
                    data = next.deref_mut().item.take();
                    guard.defer_destroy(head);
                    break;
                }
            }
        }

        self.len.fetch_sub(1, Ordering::SeqCst);
        data
    }
}

impl<T> Drop for LinkedQueue<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
        let guard = &epoch::pin();
        unsafe {
            // 释放头结点
            let h = self.head.load_consume(guard);
            guard.defer_destroy(h);
        }
    }
}

#[cfg(test)]
mod link_test {
    use super::*;

    #[test]
    fn test_simple() {
        let mut queue: LinkedQueue<_> = LinkedQueue::new();
        queue.push(2);
        assert!(queue.size() == 1);
        let op_item = queue.pop();
        assert_eq!(op_item, Some(2));
    }
}
