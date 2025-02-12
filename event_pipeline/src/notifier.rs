// notifier.rs
use crate::notifier_event::NotifierEvent;
use std::sync::Arc;
use tokio_event_bus::EventBus; // Our existing event bus implementation.

/// Notifier publishes events to an EventBus.
/// We use the concrete type: EventBus<NotifierEvent>
pub struct Notifier {
    pub bus: Option<Arc<EventBus<NotifierEvent>>>,
}

impl Notifier {
    /// Create a new notifier with an optional event bus.
    pub fn new(bus: Option<Arc<EventBus<NotifierEvent>>>) -> Self {
        Notifier { bus }
    }

    /// Create a new notifier with a new event bus.
    pub fn with_bus() -> Self {
        let bus = Arc::new(EventBus::<NotifierEvent>::new());
        Notifier { bus: Some(bus) }
    }

    /// Publish an event to the event bus.
    /// If the notifier is uninitialized, this is a no-op.
    pub async fn notify(&self, event: NotifierEvent) {
        if let Some(bus) = &self.bus {
            // Retrieve the topic using the event's topic() method.
            let topic = event.topic();

            // Create an instance of the event bus event that wraps our NotifierEvent.
            // Our original event bus expects a struct `Event<T>` with a topic and a payload.
            let bus_event = tokio_event_bus::Event {
                topic,
                payload: event, // Our NotifierEvent is the payload.
            };

            // Publish to the event bus.
            // Note: The publish method is async in your implementation.
            bus.publish(bus_event).await;
        }
    }
}
