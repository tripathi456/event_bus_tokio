# Booking Pipeline Status Example

This example demonstrates how to use tokio_event_bus with Axum to create a Server-Sent Events (SSE) endpoint for tracking booking pipeline status updates.

## Overview

The example implements:

1. An Axum server with two endpoints:
   - `GET /events/:booking_id` - SSE endpoint for subscribing to events for a specific booking ID
   - `POST /booking/status` - Endpoint for publishing booking status updates

2. A way to subscribe to events for a specific booking_id using SSE
3. A way to publish events for a booking_id

## Running the Example

To run the example, use the following command from the root of the repository:

```bash
cargo run --example pipeline --package event_pipeline
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

Available event types:
- `start` - Step started
- `completed` - Step completed
- `skipped` - Step skipped
- `pipeline_start` - Pipeline started
- `pipeline_end` - Pipeline ended
- `pipeline_abort` - Pipeline aborted

For pipeline-level events, omit the `step_name` field:

```bash
curl -X POST http://localhost:3000/booking/status \
  -H "Content-Type: application/json" \
  -d '{
    "booking_id": "booking123",
    "event_type": "pipeline_start"
  }'
```

## Testing the Example

1. Open two terminal windows
2. In the first terminal, start the server:
   ```bash
   cargo run --example pipeline --package event_pipeline
   ```
3. In the second terminal, subscribe to events:
   ```bash
   curl -N http://localhost:3000/events/booking123
   ```
4. In a third terminal, publish some events:
   ```bash
   curl -X POST http://localhost:3000/booking/status \
     -H "Content-Type: application/json" \
     -d '{
       "booking_id": "booking123",
       "step_name": "payment_processing",
       "event_type": "start"
     }'
   ```

You should see the events appear in the second terminal as they are published.