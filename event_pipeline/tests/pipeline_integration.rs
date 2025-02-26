use event_pipeline::notifier::Notifier;
use event_pipeline::notifier_event::{NotifierEvent, NotifierEventType};
use event_pipeline::pipeline::{
    process_pipeline, CreateBookingCallForTravelProvider, GetPaymentStatusFromPaymentProvider,
    MockStep, PipelineDecision, PipelineStep, SendEmailNotification, ServerSideBookingEvent,
};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use tokio_event_bus::EventBus;

#[cfg(test)]
mod test {
    use super::*;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_pipeline_with_notifier_success() {
        // Create a Notifier using the bus
        let notifier = Notifier::with_bus();

        // get the bus this notifier uses
        let bus = notifier.bus.as_ref().unwrap().clone();

        // Subscribe with a topic pattern, e.g. "booking:*"
        let (subscriber_id, subscription_receiver) = bus.subscribe("booking:*".to_string()).await;
        dbg!(&subscriber_id);
        // Wrap in a ReceiverStream so we can call .next()
        let mut subscription_stream = ReceiverStream::new(subscription_receiver);

        // 4. Create some steps
        let step1 = PipelineStep::PaymentStatus(GetPaymentStatusFromPaymentProvider);
        let step2 = PipelineStep::BookingCall(CreateBookingCallForTravelProvider);

        // 5. Prepare the input event
        let event = ServerSideBookingEvent {
            payment_id: Some("pay_123".to_string()),
            booking_id: "book_456".to_string(),
            user_email: "testuser@example.com".to_string(),
        };

        // 6. Run the pipeline
        let result = process_pipeline(event, &[step1, step2], Some(&notifier)).await;
        assert!(result.is_ok(), "Pipeline should have succeeded");

        // 7. Collect events. We expect 6 total:
        //    1) OnPipelineStart
        //    2) Step1 OnStepStart
        //    3) Step1 OnStepCompleted
        //    4) Step2 OnStepStart
        //    5) Step2 OnStepCompleted
        //    6) OnPipelineEnd
        let mut published_events = vec![];
        let mut count = 0;
        while count < 6 {
            if let Some(evt) = subscription_stream.next().await {
                // evt is an Event<NotifierEvent> { topic, payload }
                published_events.push(evt.payload);
                count += 1;
            } else {
                break;
            }
        }

        // 8. Check that we have the events we expect
        let types: Vec<NotifierEventType> = published_events
            .iter()
            .map(|e| e.event_type.clone())
            .collect();

        assert!(types.contains(&NotifierEventType::OnPipelineStart));
        assert_eq!(
            types
                .iter()
                .filter(|&t| *t == NotifierEventType::OnStepStart)
                .count(),
            2
        );
        assert_eq!(
            types
                .iter()
                .filter(|&t| *t == NotifierEventType::OnStepCompleted)
                .count(),
            2
        );
        assert!(types.contains(&NotifierEventType::OnPipelineEnd));
    }

    #[test(tokio::test)]
    async fn test_pipeline_with_notifier_skip_and_abort() {
        // 1. Create the bus
        let bus = Arc::new(EventBus::<NotifierEvent>::new());

        // 2. Subscribe with a topic pattern
        let (_subscriber_id, subscription_receiver) = bus.subscribe("booking:*".to_string()).await;
        let mut subscription_stream = ReceiverStream::new(subscription_receiver);

        // 3. Create a Notifier
        let notifier = Notifier::new(Some(bus.clone()));

        // 4. Steps: first will skip, second will abort
        let skip_step = PipelineStep::Mock(MockStep {
            decision: PipelineDecision::Skip,
            executed: Arc::new(AtomicBool::new(false)),
        });

        let abort_step = PipelineStep::Mock(MockStep {
            decision: PipelineDecision::Abort("some reason".into()),
            executed: Arc::new(AtomicBool::new(false)),
        });

        // 5. Input event
        let event = ServerSideBookingEvent {
            payment_id: None,
            booking_id: "book_999".to_string(),
            user_email: "skipabort@example.com".to_string(),
        };

        // 6. Run pipeline
        let result = process_pipeline(event, &[skip_step, abort_step], Some(&notifier)).await;
        assert!(result.is_err(), "Pipeline should abort in the second step");

        // 7. Collect events until we see OnPipelineAbort
        let mut published_events = vec![];
        while let Some(evt) = subscription_stream.next().await {
            published_events.push(evt.payload);
            if published_events.last().unwrap().event_type == NotifierEventType::OnPipelineAbort {
                break;
            }
        }

        // 8. We expect:
        //   - OnPipelineStart
        //   - OnStepSkipped   (for the first step)
        //   - OnPipelineAbort (validation for second step)
        //   *No* OnStepStart or OnStepCompleted for the aborting step
        //   and no OnPipelineEnd
        let types: Vec<NotifierEventType> = published_events
            .iter()
            .map(|e| e.event_type.clone())
            .collect();

        assert!(types.contains(&NotifierEventType::OnPipelineStart));
        assert!(types.contains(&NotifierEventType::OnStepSkipped));
        assert!(types.contains(&NotifierEventType::OnPipelineAbort));
        assert!(!types.contains(&NotifierEventType::OnPipelineEnd));
    }

    /// Test 5: Multiple Subscribers Test
    ///
    /// Scenario:
    ///   - Create two subscribers with the same topic pattern.
    ///   - Run the pipeline (with two steps) via a Notifier.
    ///   - Both subscribers should receive the same events with the same event order.
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

    /// Test 6: Pipeline Execution without Notifier Test
    ///
    /// Scenario:
    ///   - Execute the pipeline with `notifier` set to `None`.
    ///   - The pipeline should process normally but publish no events.

    #[test(tokio::test)]
    async fn test_pipeline_without_notifier() {
        // Create a bus and subscribe (though it won't be used).
        let bus = Arc::new(EventBus::<NotifierEvent>::new());
        let (_id, rx) = bus.subscribe("booking:*".to_string()).await;
        let mut stream = ReceiverStream::new(rx);

        // Run the pipeline with notifier = None.
        let step1 = PipelineStep::PaymentStatus(GetPaymentStatusFromPaymentProvider);
        let event = ServerSideBookingEvent {
            payment_id: Some("pay_789".to_string()),
            booking_id: "book_000".to_string(),
            user_email: "none_notifier@example.com".to_string(),
        };

        let result = process_pipeline(event.clone(), &[step1], None).await;
        assert!(
            result.is_ok(),
            "Pipeline should succeed even without a notifier"
        );

        // Use a timeout to check that no events are published.
        use tokio::time::{timeout, Duration};
        let event_result = timeout(Duration::from_millis(100), stream.next()).await;

        // If timeout returns Err, then no event was received within 100ms, which is what we expect.
        assert!(
            event_result.is_err(),
            "No events should be published when notifier is None"
        );
    }

    /// Test 7: Consistent Correlation ID Test
    ///
    /// Scenario:
    ///   - Execute the pipeline with a Notifier.
    ///   - All events published during the pipeline run should share the same correlation ID.
    #[test(tokio::test)]
    async fn test_consistent_correlation_id() {
        let bus = Arc::new(EventBus::<NotifierEvent>::new());
        let (_id, rx) = bus.subscribe("booking:*".to_string()).await;
        let mut stream = ReceiverStream::new(rx);

        let notifier = Notifier::new(Some(bus.clone()));

        // Define a pipeline with two steps (yielding 6 events total).
        let step1 = PipelineStep::PaymentStatus(GetPaymentStatusFromPaymentProvider);
        let step2 = PipelineStep::BookingCall(CreateBookingCallForTravelProvider);

        let event = ServerSideBookingEvent {
            payment_id: Some("pay_321".to_string()),
            booking_id: "book_654".to_string(),
            user_email: "correlation@example.com".to_string(),
        };

        let result = process_pipeline(event, &[step1, step2], Some(&notifier)).await;
        assert!(result.is_ok(), "Pipeline should succeed");

        // Collect the 6 events.
        let expected_events = 6;
        let mut events = vec![];
        for _ in 0..expected_events {
            if let Some(evt) = stream.next().await {
                events.push(evt.payload);
            }
        }

        // Verify that at least one event was received.
        assert!(
            !events.is_empty(),
            "At least one event should have been published"
        );

        // Extract the correlation ID from the first event.
        let first_corr = events[0].correlation_id.clone();
        for evt in events.iter() {
            assert_eq!(
                evt.correlation_id, first_corr,
                "All events must share the same correlation ID"
            );
        }
    }

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

    #[test(tokio::test)]
    async fn test_two_notifiers_many_subscribers() {
        // Create a shared event bus.
        let bus = Arc::new(EventBus::<NotifierEvent>::new());

        // Create multiple subscribers subscribing to all booking events.
        let (_sub1_id, rx1) = bus.subscribe("booking:*".to_string()).await;
        let (_sub2_id, rx2) = bus.subscribe("booking:*".to_string()).await;
        let (_sub3_id, rx3) = bus.subscribe("booking:*".to_string()).await;
        let mut stream1 = ReceiverStream::new(rx1);
        let mut stream2 = ReceiverStream::new(rx2);
        let mut stream3 = ReceiverStream::new(rx3);

        // Create two notifiers using the same shared bus.
        let notifier1 = Notifier::new(Some(bus.clone()));
        let notifier2 = Notifier::new(Some(bus.clone()));

        // Define a simple one-step pipeline (which produces 4 events):
        //   1. OnPipelineStart
        //   2. OnStepStart
        //   3. OnStepCompleted
        //   4. OnPipelineEnd
        let step = PipelineStep::PaymentStatus(GetPaymentStatusFromPaymentProvider);

        // Run the first pipeline with notifier1 for booking "book_111".
        let event1 = ServerSideBookingEvent {
            payment_id: Some("pay_111".to_string()),
            booking_id: "book_111".to_string(),
            user_email: "user1@example.com".to_string(),
        };
        let res1 = process_pipeline(event1, &[step.clone()], Some(&notifier1)).await;
        assert!(res1.is_ok(), "Pipeline for book_111 should succeed");

        // Run the second pipeline with notifier2 for booking "book_222".
        let event2 = ServerSideBookingEvent {
            payment_id: Some("pay_222".to_string()),
            booking_id: "book_222".to_string(),
            user_email: "user2@example.com".to_string(),
        };
        let res2 = process_pipeline(event2, &[step.clone()], Some(&notifier2)).await;
        assert!(res2.is_ok(), "Pipeline for book_222 should succeed");

        // Each pipeline produces 4 events, so total events expected per subscriber: 4 + 4 = 8.
        let expected_events = 8;

        let mut events_sub1 = Vec::new();
        let mut events_sub2 = Vec::new();
        let mut events_sub3 = Vec::new();

        for _ in 0..expected_events {
            if let Some(evt) = stream1.next().await {
                events_sub1.push(evt.payload);
            }
            if let Some(evt) = stream2.next().await {
                events_sub2.push(evt.payload);
            }
            if let Some(evt) = stream3.next().await {
                events_sub3.push(evt.payload);
            }
        }

        // For each subscriber, verify that there are 4 events for booking "book_111" and 4 events for "book_222".
        for (i, events) in [events_sub1, events_sub2, events_sub3].iter().enumerate() {
            let book_111_count = events.iter().filter(|e| e.booking_id == "book_111").count();
            let book_222_count = events.iter().filter(|e| e.booking_id == "book_222").count();
            assert_eq!(
                book_111_count,
                4,
                "Subscriber {} should receive 4 events for book_111",
                i + 1
            );
            assert_eq!(
                book_222_count,
                4,
                "Subscriber {} should receive 4 events for book_222",
                i + 1
            );
        }
    }

    /// Test for sending email via pipeline
    ///
    /// Scenario:
    ///   - Create a pipeline with a SendEmail step
    ///   - Run the pipeline via a Notifier
    ///   - Verify that the pipeline completes successfully
    ///   - Verify that the correct events are published
    #[test(tokio::test)]
    async fn test_pipeline_with_email_notification() {
        // Create a Notifier using the bus
        let notifier = Notifier::with_bus();

        // get the bus this notifier uses
        let bus = notifier.bus.as_ref().unwrap().clone();

        // Subscribe with a topic pattern
        let (subscriber_id, subscription_receiver) = bus.subscribe("booking:*".to_string()).await;
        dbg!(&subscriber_id);
        // Wrap in a ReceiverStream so we can call .next()
        let mut subscription_stream = ReceiverStream::new(subscription_receiver);

        // Create a pipeline with a SendEmail step
        let step = PipelineStep::SendEmail(SendEmailNotification);

        // Prepare the input event
        let event = ServerSideBookingEvent {
            payment_id: None, // Not needed for email
            booking_id: "book_789".to_string(),
            user_email: "test_email@example.com".to_string(),
        };

        // Run the pipeline
        let result = process_pipeline(event, &[step], Some(&notifier)).await;
        assert!(result.is_ok(), "Pipeline should have succeeded");

        // Collect events. We expect 4 total for a single step:
        //    1) OnPipelineStart
        //    2) OnStepStart
        //    3) OnStepCompleted
        //    4) OnPipelineEnd
        let mut published_events = vec![];
        let mut count = 0;
        while count < 4 {
            if let Some(evt) = subscription_stream.next().await {
                published_events.push(evt.payload);
                count += 1;
            } else {
                break;
            }
        }

        // Check that we have the events we expect
        let types: Vec<NotifierEventType> = published_events
            .iter()
            .map(|e| e.event_type.clone())
            .collect();

        assert!(types.contains(&NotifierEventType::OnPipelineStart));
        assert!(types.contains(&NotifierEventType::OnStepStart));
        assert!(types.contains(&NotifierEventType::OnStepCompleted));
        assert!(types.contains(&NotifierEventType::OnPipelineEnd));

        // Verify that the step name in the events is correct
        let step_events: Vec<&NotifierEvent> = published_events
            .iter()
            .filter(|e| e.step_name.is_some())
            .collect();
        
        for evt in step_events {
            assert_eq!(
                evt.step_name.as_ref().unwrap(),
                "SendEmailNotification",
                "Step name should be SendEmailNotification"
            );
        }
    }

    /// Test for a complete booking workflow with email notification
    ///
    /// Scenario:
    ///   - Create a pipeline with multiple steps: payment status check, booking call, and email notification
    ///   - Run the pipeline via a Notifier
    ///   - Verify that the pipeline completes successfully
    ///   - Verify that the correct events are published in the expected order
    #[test(tokio::test)]
    async fn test_complete_booking_workflow_with_email() {
        // Create a Notifier using the bus
        let notifier = Notifier::with_bus();

        // get the bus this notifier uses
        let bus = notifier.bus.as_ref().unwrap().clone();

        // Subscribe with a topic pattern
        let (subscriber_id, subscription_receiver) = bus.subscribe("booking:*".to_string()).await;
        dbg!(&subscriber_id);
        // Wrap in a ReceiverStream so we can call .next()
        let mut subscription_stream = ReceiverStream::new(subscription_receiver);

        // Create a pipeline with multiple steps
        let step1 = PipelineStep::PaymentStatus(GetPaymentStatusFromPaymentProvider);
        let step2 = PipelineStep::BookingCall(CreateBookingCallForTravelProvider);
        let step3 = PipelineStep::SendEmail(SendEmailNotification);

        // Prepare the input event
        let event = ServerSideBookingEvent {
            payment_id: Some("pay_workflow_123".to_string()),
            booking_id: "book_workflow_456".to_string(),
            user_email: "workflow_test@example.com".to_string(),
        };

        // Run the pipeline
        let result = process_pipeline(event, &[step1, step2, step3], Some(&notifier)).await;
        assert!(result.is_ok(), "Pipeline should have succeeded");

        // Collect events. We expect 8 total for three steps:
        //    1) OnPipelineStart
        //    2) Step1 OnStepStart
        //    3) Step1 OnStepCompleted
        //    4) Step2 OnStepStart
        //    5) Step2 OnStepCompleted
        //    6) Step3 OnStepStart
        //    7) Step3 OnStepCompleted
        //    8) OnPipelineEnd
        let mut published_events = vec![];
        let mut count = 0;
        while count < 8 {
            if let Some(evt) = subscription_stream.next().await {
                published_events.push(evt.payload);
                count += 1;
            } else {
                break;
            }
        }

        // Check that we have the events we expect
        let types: Vec<NotifierEventType> = published_events
            .iter()
            .map(|e| e.event_type.clone())
            .collect();

        assert!(types.contains(&NotifierEventType::OnPipelineStart));
        assert_eq!(
            types
                .iter()
                .filter(|&t| *t == NotifierEventType::OnStepStart)
                .count(),
            3
        );
        assert_eq!(
            types
                .iter()
                .filter(|&t| *t == NotifierEventType::OnStepCompleted)
                .count(),
            3
        );
        assert!(types.contains(&NotifierEventType::OnPipelineEnd));

        // Verify the order of events
        assert_eq!(types[0], NotifierEventType::OnPipelineStart);
        assert_eq!(types[1], NotifierEventType::OnStepStart); // Step1
        assert_eq!(types[2], NotifierEventType::OnStepCompleted); // Step1
        assert_eq!(types[3], NotifierEventType::OnStepStart); // Step2
        assert_eq!(types[4], NotifierEventType::OnStepCompleted); // Step2
        assert_eq!(types[5], NotifierEventType::OnStepStart); // Step3
        assert_eq!(types[6], NotifierEventType::OnStepCompleted); // Step3
        assert_eq!(types[7], NotifierEventType::OnPipelineEnd);

        // Verify the step names in the events
        let step_events: Vec<&NotifierEvent> = published_events
            .iter()
            .filter(|e| e.step_name.is_some())
            .collect();
        
        assert_eq!(
            step_events[0].step_name.as_ref().unwrap(),
            "GetPaymentStatusFromPaymentProvider"
        );
        assert_eq!(
            step_events[1].step_name.as_ref().unwrap(),
            "GetPaymentStatusFromPaymentProvider"
        );
        assert_eq!(
            step_events[2].step_name.as_ref().unwrap(),
            "CreateBookingCallForTravelProvider"
        );
        assert_eq!(
            step_events[3].step_name.as_ref().unwrap(),
            "CreateBookingCallForTravelProvider"
        );
        assert_eq!(
            step_events[4].step_name.as_ref().unwrap(),
            "SendEmailNotification"
        );
        assert_eq!(
            step_events[5].step_name.as_ref().unwrap(),
            "SendEmailNotification"
        );
    }
}