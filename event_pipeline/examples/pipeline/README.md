# Booking Pipeline Status Example

This example demonstrates how to use tokio_event_bus with Axum to create a Server-Sent Events (SSE) endpoint for tracking booking pipeline status updates.

## Overview

The example implements:

1. An Axum server with three endpoints:
   - `GET /events/:booking_id` - SSE endpoint for subscribing to events for a specific booking ID
   - `POST /booking/status` - Endpoint for publishing booking status updates
   - `GET /pipeline/:booking_id` - Endpoint for running a pipeline with MockSteps

2. A way to subscribe to events for a specific booking_id using SSE
3. A way to publish events for a booking_id
4. A demonstration of running a pipeline with MockSteps that have different behaviors

## Running the Example

To run the example, use the following command from the root of the repository:

```bash
cargo run --example pipeline --package event_pipeline --features=examples
```

The server will start on http://localhost:3000.

## Using the Example

### Subscribe to Booking Events

To subscribe to events for a specific booking ID, open a browser or use a tool like `curl` to connect to the SSE endpoint:

```bash
curl -N http://localhost:3000/events/booking123
```

This will establish an SSE connection that will receive events related to booking ID "booking123".

### Publish Booking Status Updates

To publish a booking status update, send a POST request to the `/booking/status` endpoint:

```bash
curl -X POST http://localhost:3000/booking/status \
  -H "Content-Type: application/json" \
  -d '{
    "booking_id": "booking123",
    "step_name": "payment_processing",
    "event_type": "start"
  }'
```

Valid event types include:
- `start` - Step started
- `completed` - Step completed
- `skipped` - Step skipped
- `pipeline_start` - Pipeline started
- `pipeline_end` - Pipeline ended
- `pipeline_abort` - Pipeline aborted

### Run a Pipeline with MockSteps

To run a pipeline with MockSteps, send a GET request to the `/pipeline/:booking_id` endpoint:

```bash
curl http://localhost:3000/pipeline/booking123
```

This will run a pipeline with three MockSteps:
1. Step 1: Always runs (PipelineDecision::Run)
2. Step 2: Always skips (PipelineDecision::Skip)
3. Step 3: Runs if the booking ID doesn't contain "fail", otherwise aborts

To test the abort behavior, include "fail" in the booking ID:

```bash
curl http://localhost:3000/pipeline/fail_test
```

The pipeline will emit events that you can observe through the SSE connection. The events include:
- OnPipelineStart
- OnStepStart (for steps that run)
- OnStepCompleted (for steps that run)
- OnStepSkipped (for steps that are skipped)
- OnPipelineEnd (if the pipeline completes successfully)
- OnPipelineAbort (if the pipeline is aborted)

## How It Works

### Pipeline with MockSteps

The pipeline uses MockStep, which is a test utility that allows controlling pipeline behavior with predefined decisions:
- `PipelineDecision::Run` - The step will run normally
- `PipelineDecision::Skip` - The step will be skipped
- `PipelineDecision::Abort(reason)` - The pipeline will abort with the given reason

Each MockStep has:
- A `decision` that determines how the step behaves
- An `executed` flag that tracks whether the step was executed

The pipeline handler creates a ServerSideBookingEvent from the booking ID and runs the pipeline with the MockSteps. The results are returned as a JSON response.