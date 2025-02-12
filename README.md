# Event Pipeline Crate

This crate provides a flexible and extensible event pipeline for processing events in a structured manner. It enables you to define a series of steps that an event will pass through, with each step potentially validating, modifying, or enriching the event.  It leverages the `tokio_event_bus` crate for inter-component communication and notification.

## Core Concepts

*   **Event:**  A data structure representing the core information being processed by the pipeline.  An example, `ServerSideBookingEvent`, is provided in the crate.

*   **Pipeline Step:** A discrete unit of work within the pipeline.  Each step can implement validation logic to determine whether it should be executed, skipped, or if the entire pipeline should be aborted.

*   **Pipeline Decision:** An enum that dictates the behavior of the pipeline based on validation results.  Options include:
    *   `Run`: Execute the step.
    *   `Skip`: Skip the step and proceed to the next.
    *   `Abort`: Terminate the pipeline with an error.

*   **Pipeline Validator:**  A trait defining the `validate` method, which determines the `PipelineDecision` for a given step.

*   **Pipeline Executor:**  A trait defining the `execute` method, which performs the actual work of a step, potentially modifying the event.

*   **Notifier:**  A component that leverages the `tokio_event_bus` to publish events at various stages of the pipeline's execution, allowing for real-time monitoring and auditing. This uses the `NotifierEvent` which is a wrapper around the different types of `NotifierEventType` used in the application, such as `OnStepStart`, `OnStepCompleted`, `OnPipelineStart`, etc.

*   **EventBus:** The underlying message bus, provided by the tokio-event-bus crate, that facilitates communication between the pipeline and subscribers.

## Features

*   **Extensible Pipeline:**  Easily add, remove, or reorder pipeline steps to adapt to evolving requirements.
*   **Validation and Control Flow:**  Fine-grained control over pipeline execution based on event data and step-specific validation logic.
*   **Event-Driven Notifications:**  Receive real-time notifications about pipeline progress and outcomes via the `Notifier` and `tokio_event_bus`.
*   **Correlation ID Tracking:**  All events published during a pipeline run share a unique correlation ID, enabling easier tracing and debugging.
*   **Testability:**  Includes a `MockStep` for simplified unit testing of pipeline logic.
*   **Flexibility:** Can be used with or without a `Notifier`, allowing integration into environments where external notification is not required.

## Getting Started

1.  **Add the dependency:**

    ```toml
    [dependencies]
    event_pipeline = "0.1.0" # Replace with the latest version
    tokio_event_bus = "0.1.0" # Or higher, depending on compatibility
    ```

2.  **Define your event:**

    ```rust
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct MyEvent {
        pub id: String,
        pub data: String,
    }
    ```

3.  **Implement your pipeline steps:**

    ```rust
    use async_trait::async_trait;
    use event_pipeline::pipeline::{
        PipelineValidator, PipelineExecutor, ServerSideBookingEvent, PipelineDecision
    };

    #[derive(Debug, Clone)]
    pub struct MyStep;

    #[async_trait]
    impl PipelineValidator for MyStep {
        async fn validate(&self, event: &ServerSideBookingEvent) -> Result<PipelineDecision, String> {
            if event.payment_id.is_some() {
                Ok(PipelineDecision::Run)
            } else {
                Ok(PipelineDecision::Skip)
            }
        }
    }

    #[async_trait]
    impl PipelineExecutor for MyStep {
        async fn execute(mut event: ServerSideBookingEvent) -> Result<ServerSideBookingEvent, String> {
            println!("Executing MyStep");
            event.user_email = "some other email".to_string();
            Ok(event)
        }
    }
    ```

4.  **Create your pipeline:**

    ```rust
    use event_pipeline::pipeline::{PipelineStep, process_pipeline};
    use event_pipeline::notifier::Notifier;

    #[tokio::main]
    async fn main() -> Result<(), String>{
        let event = ServerSideBookingEvent {
            payment_id: Some("pay_123".to_string()),
            booking_id: "book_456".to_string(),
            user_email: "testuser@example.com".to_string(),
        };

        let step1 = PipelineStep::PaymentStatus(event_pipeline::pipeline::GetPaymentStatusFromPaymentProvider);
        // let step2 = PipelineStep::Mock(mock_step); // example of using mock
        let steps = vec![step1];

        let notifier = Notifier::with_bus(); // Or Notifier::new(None) for no notifications
        let result = process_pipeline(event, &steps, Some(¬ifier)).await?;

        println!("Pipeline completed successfully: {:?}", result);
        Ok(())
    }
    ```

## Advanced Usage

*   **Subscribing to Events:** Use the `tokio_event_bus` directly to subscribe to specific topics and receive notifications from the pipeline.
*   **Custom Event Types:**  Define your own event types tailored to your application's domain.
*   **Complex Validation Logic:** Implement sophisticated validation rules within your pipeline steps using external services or data sources.
*   **Error Handling:** Customize error handling within pipeline steps to gracefully manage exceptions and prevent pipeline failures.
*   **Asynchronous Operations:** Leverage `async` and `await` for non-blocking I/O and parallel processing within your pipeline steps.

More in depth documentation can be found [here](ADVANCED_USAGE.md).

## Testing

The crate includes integration tests under the `event_pipeline/tests/pipeline_integration.rs` file. Run them with:

```bash
cargo test --package **event_pipeline**