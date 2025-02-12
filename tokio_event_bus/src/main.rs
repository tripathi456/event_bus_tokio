use std::sync::Arc;
use tokio_event_bus::{Event, EventBus};
use tracing_subscriber;

#[tokio::main]
async fn main() {
    // Initialize tracing for logging.
    tracing_subscriber::fmt::init();

    // Create an Arc-wrapped EventBus that uses String as the payload.
    let event_bus = Arc::new(EventBus::<String>::new());

    // Subscriber registers for all booking-related events using a wildcard pattern.
    let (subscriber_id, mut receiver) = event_bus.subscribe("booking:*".to_string()).await;
    println!(
        "Subscriber {} registered for booking events.",
        subscriber_id
    );

    // Spawn a task to listen for events on the subscriber's receiver.
    let bus_clone = Arc::clone(&event_bus);
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            println!("Subscriber {} received event: {:?}", subscriber_id, event);
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
