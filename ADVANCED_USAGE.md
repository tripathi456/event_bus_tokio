
# Event Pipeline Crate - Advanced Features and Usage

This document highlights the advanced features of the `event_pipeline` crate, demonstrating how to leverage multiple subscribers, wildcard matching, and other functionalities to build robust and flexible event processing pipelines. The examples provided are based on the integration tests located in `event_pipeline/tests/pipeline_integration.rs`.

## Multiple Subscribers

The `event_pipeline` crate, combined with `tokio_event_bus`, supports multiple subscribers listening for events published by the pipeline's `Notifier`. This allows for different parts of your system to react to the same events in different ways, enabling powerful monitoring, auditing, and side-effect processing.

**Key Concepts:**

*   **Shared EventBus:** An `EventBus` instance wrapped in an `Arc` is shared between the pipeline and multiple subscribers.
*   **Independent Receivers:** Each subscriber obtains its own `Receiver` from the `EventBus` by calling the `subscribe` method.
*   **Parallel Processing:** Subscribers process events independently and concurrently.

**Example:**

```rust
#[cfg(test)]
mod test {
    use super::*;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_multiple_subscribers() {
        // Create a shared event bus.
        let bus = Arc::new(EventBus::<NotifierEvent>::new());

        // Subscribe two subscribers with the same topic pattern.
        let (_id1, rx1) = bus.subscribe("booking:*".to_string()).await;
        let (_id2, rx2) = bus.subscribe("booking:*".to_string()).await;
        let mut stream1 = ReceiverStream::new(rx1);
        let mut stream2 = ReceiverStream::new(rx2);

        // Create a Notifier that wraps the bus.
        let notifier = Notifier::new(Some(bus.clone()));

        // Define a pipeline with two steps.
        let step1 = PipelineStep::PaymentStatus(GetPaymentStatusFromPaymentProvider);
        let step2 = PipelineStep::BookingCall(CreateBookingCallForTravelProvider);

        let event = ServerSideBookingEvent {
            payment_id: Some("pay_123".to_string()),
            booking_id: "book_456".to_string(),
            user_email: "testuser@example.com".to_string(),
        };

        let result = process_pipeline(event, &[step1, step2], Some(&notifier)).await;
        assert!(result.is_ok(), "Pipeline should have succeeded");

        // We expect a total of 6 events:
        //   1. OnPipelineStart
        //   2. Step1 OnStepStart
        //   3. Step1 OnStepCompleted
        //   4. Step2 OnStepStart
        //   5. Step2 OnStepCompleted
        //   6. OnPipelineEnd
        let expected_events = 6;
        let mut events_sub1 = vec![];
        let mut events_sub2 = vec![];

        for _ in 0..expected_events {
            if let Some(evt) = stream1.next().await {
                events_sub1.push(evt.payload);
            }
            if let Some(evt) = stream2.next().await {
                events_sub2.push(evt.payload);
            }
        }

        assert_eq!(
            events_sub1.len(),
            expected_events,
            "Subscriber 1 should receive all events"
        );
        assert_eq!(
            events_sub2.len(),
            expected_events,
            "Subscriber 2 should receive all events"
        );

        // Compare the event types (order should be the same).
        let types1: Vec<_> = events_sub1.iter().map(|e| e.event_type.clone()).collect();
        let types2: Vec<_> = events_sub2.iter().map(|e| e.event_type.clone()).collect();

        assert_eq!(
            types1, types2,
            "Both subscribers should see identical event types in order"
        );
    }
}

```

This test sets up two subscribers to the same `EventBus` instance, both listening for events with the `"booking:*"` topic pattern. The pipeline is then executed, and the test asserts that both subscribers receive the same sequence of events, demonstrating the ability to broadcast pipeline events to multiple consumers.

## Wildcard Matching

The `tokio_event_bus` supports wildcard matching in topic subscriptions. This allows subscribers to listen for a range of events based on a pattern.  Currently, only a trailing `*` is supported, representing a prefix match.

**Example:**

A subscriber subscribing to `"booking:book_456:*"` will receive events with topics such as:

*   `"booking:book_456:step:GetPaymentStatusFromPaymentProvider:on_step_start"`
*   `"booking:book_456:step:CreateBookingCallForTravelProvider:on_step_completed"`
*   `"booking:book_456:step:*:on_pipeline_end"` (Note the `*` placeholder for step name).

However, it **will not** receive events with topics such as:

*   `"booking:book_789:step:GetPaymentStatusFromPaymentProvider:on_step_start"`
*   `"order:book_456:step:GetPaymentStatusFromPaymentProvider:on_step_start"`

**Usage in the `event_pipeline` Crate:**

The `NotifierEvent` struct has a `topic()` method that generates topics in the format `"booking:{booking_id}:step:{step_name}:{event_type}"`. This structured topic format makes it easy to use wildcard matching to filter events based on booking ID, step name, or event type.

**Example Test Scenario:**

```rust
#[cfg(test)]
mod test {
    use super::*;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_topic_pattern_matching() {
        // Create a shared event bus.
        let bus = Arc::new(EventBus::<NotifierEvent>::new());

        // Subscriber for booking "book_456" only.
        let (_sub_id1, rx1) = bus.subscribe("booking:book_456:*".to_string()).await;
        let mut stream1 = ReceiverStream::new(rx1);

        // Subscriber for booking "book_789" only.
        let (_sub_id2, rx2) = bus.subscribe("booking:book_789:*".to_string()).await;
        let mut stream2 = ReceiverStream::new(rx2);

        // Create a Notifier that wraps the bus.
        let notifier = Notifier::new(Some(bus.clone()));

        // Define a simple one-step pipeline (expected 4 events: PipelineStart, StepStart, StepCompleted, PipelineEnd)
        let step = PipelineStep::PaymentStatus(GetPaymentStatusFromPaymentProvider);

        // Run pipeline for booking "book_456".
        let event1 = ServerSideBookingEvent {
            payment_id: Some("pay_456".to_string()),
            booking_id: "book_456".to_string(),
            user_email: "user1@example.com".to_string(),
        };
        let res1 = process_pipeline(event1, &[step.clone()], Some(&notifier)).await;
        assert!(res1.is_ok(), "Pipeline for book_456 should succeed");

        // Run pipeline for booking "book_789".
        let event2 = ServerSideBookingEvent {
            payment_id: Some("pay_789".to_string()),
            booking_id: "book_789".to_string(),
            user_email: "user2@example.com".to_string(),
        };
        let res2 = process_pipeline(event2, &[step], Some(&notifier)).await;
        assert!(res2.is_ok(), "Pipeline for book_789 should succeed");

        // For a one-step pipeline, we expect 4 events per run.
        // Subscriber 1 should only receive events for booking "book_456".
        let mut events_sub1 = Vec::new();
        for _ in 0..4 {
            if let Some(evt) = stream1.next().await {
                events_sub1.push(evt.payload);
            }
        }
        for evt in events_sub1 {
            assert_eq!(
                evt.booking_id, "book_456",
                "Subscriber 1 should only receive events for booking book_456"
            );
        }

        // Subscriber 2 should only receive events for booking "book_789".
        let mut events_sub2 = Vec::new();
        for _ in 0..4 {
            if let Some(evt) = stream2.next().await {
                events_sub2.push(evt.payload);
            }
        }
        for evt in events_sub2 {
            assert_eq!(
                evt.booking_id, "book_789",
                "Subscriber 2 should only receive events for booking book_789"
            );
        }
    }
}
```

This test sets up two subscribers, each listening for events related to a specific booking ID using the `booking:booking_id:*` topic pattern. It then runs the pipeline twice, once for each booking ID, and asserts that each subscriber only receives events for its respective booking.

## Other Notable Features (as demonstrated in tests)

*   **Pipeline Execution Without Notifier:** The `process_pipeline` function accepts an optional `Notifier`. If `None` is provided, the pipeline will execute without publishing any events, allowing you to use the pipeline purely for its processing logic without the overhead of event notifications.  Useful in situations such as testing, or where events are only required in specific situations
*   **Consistent Correlation ID:**  A unique `correlation_id` is generated at the start of each pipeline execution and attached to all `NotifierEvent`s published during that run. This ensures that all events related to a single pipeline invocation can be easily grouped and tracked, facilitating debugging and auditing. The `test_consistent_correlation_id` demonstrates this behaviour.
*   **Step Skipping and Aborting:** The use of `PipelineDecision::Skip` and `PipelineDecision::Abort` allows fine-grained control over the pipeline's execution flow. Steps can be skipped based on validation logic, and the entire pipeline can be aborted if a critical error is encountered. The `test_pipeline_with_notifier_skip_and_abort` demonstrates this feature and the events that are/aren't published.

By combining these advanced features, the `event_pipeline` crate empowers you to build sophisticated event processing systems with flexible routing, real-time monitoring, and robust error handling. Remember to explore the integration tests in `event_pipeline/tests/pipeline_integration.rs` for more detailed examples and usage scenarios.
