use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, RwLock};
use tracing::warn;

/// An event with a topic and a generic payload.
#[derive(Debug, Clone)]
pub struct Event<T: Clone + std::fmt::Debug + Send + 'static> {
    pub topic: String, // e.g., "booking:123"
    pub payload: T,    // generic payload type
}

/// A subscription holding a topic pattern and a bounded sender channel for events.
pub struct Subscription<T: Clone + std::fmt::Debug + Send + 'static> {
    pub pattern: String,                // e.g., "booking:*"
    pub sender: mpsc::Sender<Event<T>>, // bounded channel for events
}

/// The EventBus holds a registry of subscriptions and auto-generates subscriber IDs.
pub struct EventBus<T: Clone + std::fmt::Debug + Send + 'static> {
    subscriptions: RwLock<HashMap<usize, Subscription<T>>>,
    next_id: AtomicUsize,
}

impl<T: Clone + std::fmt::Debug + Send + 'static> EventBus<T> {
    /// Creates a new EventBus.
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            next_id: AtomicUsize::new(1),
        }
    }

    /// Subscribe to events matching the given topic pattern.
    /// Returns the auto-generated subscriber id and a receiver for events.
    pub async fn subscribe(&self, pattern: String) -> (usize, mpsc::Receiver<Event<T>>) {
        // Create a bounded channel with capacity 1000.
        let (sender, receiver) = mpsc::channel(1000);
        let subscription = Subscription { pattern, sender };

        // Generate a unique subscriber id.
        let subscriber_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut subs = self.subscriptions.write().await;
        subs.insert(subscriber_id, subscription);

        (subscriber_id, receiver)
    }

    /// Unsubscribe using the generated subscriber id.
    pub async fn unsubscribe(&self, subscriber_id: usize) {
        let mut subs = self.subscriptions.write().await;
        subs.remove(&subscriber_id);
    }

    /// Publish an event to all matching subscriptions.
    pub async fn publish(&self, event: Event<T>) {
        let subs = self.subscriptions.read().await;
        for subscription in subs.values() {
            let pattern = &subscription.pattern;
            // Simple wildcard matching: if pattern ends with '*', do a prefix match.
            let is_match = if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                event.topic.starts_with(prefix)
            } else {
                event.topic == *pattern
            };

            if is_match {
                // Try to send the event; log a warning if the channel is full.
                if let Err(err) = subscription.sender.try_send(event.clone()) {
                    warn!(
                        "Dropping event on topic '{}' for pattern '{}'. Error: {:?}",
                        event.topic, pattern, err
                    );
                }
            }
        }
    }
}
