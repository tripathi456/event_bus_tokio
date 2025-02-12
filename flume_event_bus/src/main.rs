// https://chatgpt.com/share/67ac7e39-aef4-8010-b078-2110356d3718

use flume;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;
use tracing_subscriber;

/// An event with a topic and a generic payload.
#[derive(Debug, Clone)]
pub struct Event<T: Clone + std::fmt::Debug + Send + 'static> {
    pub topic: String, // e.g., "booking:123"
    pub payload: T,    // generic payload type
}

/// A subscription holding a topic pattern and a bounded sender channel for events.
pub struct Subscription<T: Clone + std::fmt::Debug + Send + 'static> {
    pub pattern: String,                 // e.g., "booking:*"
    pub sender: flume::Sender<Event<T>>, // bounded channel for events
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
    pub async fn subscribe(&self, pattern: String) -> (usize, flume::Receiver<Event<T>>) {
        // Create a bounded flume channel with capacity 1000.
        let (sender, receiver) = flume::bounded(1000);
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

#[tokio::main]
async fn main() {
    // Initialize tracing for logging.
    tracing_subscriber::fmt::init();

    // Create an Arc-wrapped EventBus that uses String as the payload.
    let event_bus = Arc::new(EventBus::<String>::new());

    // Subscriber registers for all booking-related events using a wildcard pattern.
    let (subscriber_id, receiver) = event_bus.subscribe("booking:*".to_string()).await;
    println!(
        "Subscriber {} registered for booking events.",
        subscriber_id
    );

    // Spawn a task to listen for events on the subscriber's receiver.
    // (Here, we use flume's async receiver API.)
    let subscriber_id_clone = subscriber_id;
    tokio::spawn(async move {
        while let Ok(event) = receiver.recv_async().await {
            println!(
                "Subscriber {} received event: {:?}",
                subscriber_id_clone, event
            );
        }
    });

    // Publisher publishes an event.
    let event = Event {
        topic: "booking:123".to_string(),
        payload: "Booking confirmed".to_string(),
    };

    event_bus.publish(event).await;

    // Allow some time for the spawned task to process the event before the program exits.
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}
