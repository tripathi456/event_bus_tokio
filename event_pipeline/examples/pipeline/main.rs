use axum::{
    extract::{Path, State},
    response::{sse::{Event, Sse}, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use event_pipeline::{
    notifier::Notifier,
    notifier_event::{NotifierEvent, NotifierEventType},
    pipeline::{
        process_pipeline, MockStep, PipelineDecision, PipelineStep, ServerSideBookingEvent,
    },
    uuidv7,
};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, net::SocketAddr, sync::Arc, sync::atomic::AtomicBool, time::Duration};
use tokio_event_bus::EventBus;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

// Shared application state
struct AppState {
    notifier: Arc<Notifier>,
}

// Request payload for creating a booking status update
#[derive(Debug, Deserialize)]
struct BookingStatusUpdate {
    booking_id: String,
    step_name: Option<String>,
    event_type: String,
}

// Response for a successful booking status update
#[derive(Debug, Serialize)]
struct BookingStatusResponse {
    message: String,
    event_id: String,
}

// Response for a pipeline execution
#[derive(Debug, Serialize)]
struct PipelineResponse {
    message: String,
    success: bool,
    details: String,
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Create event bus and notifier
    let event_bus = Arc::new(EventBus::<NotifierEvent>::new());
    let notifier = Arc::new(Notifier::new(Some(event_bus)));

    // Create shared state
    let state = Arc::new(AppState {
        notifier: notifier.clone(),
    });

    // Build our application with routes
    let app = Router::new()
        .route("/events/:booking_id", get(sse_handler))
        .route("/booking/status", post(update_booking_status))
        .route("/pipeline/:booking_id", get(pipeline_handler))
        .with_state(state);

    // Run the server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// Handler for SSE endpoint
async fn sse_handler(
    Path(booking_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    info!("New client connected for booking_id: {}", booking_id);

    // Create a pattern to match events for this booking_id
    let pattern = format!("booking:{}:*", booking_id);

    // Subscribe to events
    let (_subscriber_id, mut receiver) = state
        .notifier
        .bus
        .as_ref()
        .unwrap()
        .subscribe(pattern)
        .await;

    // Create a stream that maps events to SSE events
    let stream = async_stream::stream! {
        loop {
            match receiver.recv().await {
                Some(event) => {
                    let payload = serde_json::to_string(&event.payload).unwrap();
                    yield Ok::<_, Infallible>(Event::default().data(payload));
                }
                None => break,
            }
        }
    };

    // Return the SSE response
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(1))
            .text("keep-alive-text"),
    )
}

// Handler for updating booking status
async fn update_booking_status(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BookingStatusUpdate>,
) -> impl IntoResponse {
    info!("Received booking status update: {:?}", payload);

    // Map the event type string to NotifierEventType
    let event_type = match payload.event_type.as_str() {
        "start" => NotifierEventType::OnStepStart,
        "completed" => NotifierEventType::OnStepCompleted,
        "skipped" => NotifierEventType::OnStepSkipped,
        "pipeline_start" => NotifierEventType::OnPipelineStart,
        "pipeline_end" => NotifierEventType::OnPipelineEnd,
        "pipeline_abort" => NotifierEventType::OnPipelineAbort,
        _ => NotifierEventType::OnStepStart, // Default
    };

    // Create a correlation ID
    let correlation_id = uuidv7::create();

    // Create the notifier event
    let event = if let Some(step_name) = payload.step_name {
        match event_type {
            NotifierEventType::OnStepStart => {
                NotifierEvent::new_step_start(payload.booking_id.clone(), step_name, correlation_id)
            }
            _ => {
                // For simplicity, we're using new_step_start for all event types
                // In a real application, you would use the appropriate constructor
                NotifierEvent::new_step_start(payload.booking_id.clone(), step_name, correlation_id)
            }
        }
    } else {
        // For pipeline-level events (no step name)
        NotifierEvent {
            event_id: uuidv7::create(),
            correlation_id,
            timestamp: chrono::Utc::now(),
            booking_id: payload.booking_id.clone(),
            step_name: None,
            event_type,
        }
    };

    // Get the event ID for the response
    let event_id = event.event_id.clone();

    // Publish the event
    state.notifier.notify(event).await;

    // Return a success response
    Json(BookingStatusResponse {
        message: "Booking status updated successfully".to_string(),
        event_id,
    })
}

// Handler for running a pipeline with MockSteps
async fn pipeline_handler(
    Path(booking_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    info!("Running pipeline for booking_id: {}", booking_id);

    // Create a ServerSideBookingEvent from the booking_id
    let event = ServerSideBookingEvent {
        payment_id: Some("pay_".to_string() + &booking_id),
        booking_id: booking_id.clone(),
        user_email: "test@example.com".to_string(),
    };

    // Create three MockSteps with different behaviors
    let step1 = PipelineStep::Mock(MockStep {
        decision: PipelineDecision::Run,
        executed: Arc::new(AtomicBool::new(false)),
    });

    let step2 = PipelineStep::Mock(MockStep {
        decision: PipelineDecision::Skip,
        executed: Arc::new(AtomicBool::new(false)),
    });

    let step3 = PipelineStep::Mock(MockStep {
        decision: if booking_id.contains("fail") {
            PipelineDecision::Abort("Booking ID contains 'fail'".into())
        } else {
            PipelineDecision::Run
        },
        executed: Arc::new(AtomicBool::new(false)),
    });

    // Run the pipeline
    let result = process_pipeline(event, &[step1, step2, step3], Some(&state.notifier)).await;

    // Return a response based on the pipeline result
    match result {
        Ok(_) => Json(PipelineResponse {
            message: "Pipeline completed successfully".to_string(),
            success: true,
            details: "All steps processed. Step 1 executed, Step 2 skipped, Step 3 executed.".to_string(),
        }),
        Err(reason) => Json(PipelineResponse {
            message: "Pipeline failed".to_string(),
            success: false,
            details: reason,
        }),
    }
}