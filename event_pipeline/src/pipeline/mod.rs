use crate::notifier::Notifier;
use crate::notifier_event::{NotifierEvent, NotifierEventType};
use crate::uuidv7;
use chrono::Utc;

mod pipeline_utils;
pub use pipeline_utils::*;

/// Process the pipeline of steps in order, optionally publishing events via `notifier`.
pub async fn process_pipeline(
    event: ServerSideBookingEvent,
    steps: &[PipelineStep],
    notifier: Option<&Notifier>,
) -> Result<ServerSideBookingEvent, String> {
    let mut current_event = event;

    // Generate a correlation_id for this pipeline run.
    let correlation_id = uuidv7::create();

    // 1. Publish OnPipelineStart
    if let Some(n) = notifier {
        let pipeline_start_event = NotifierEvent {
            event_id: uuidv7::create(),
            correlation_id: correlation_id.clone(),
            timestamp: Utc::now(),
            booking_id: current_event.booking_id.clone(),
            step_name: None,
            event_type: NotifierEventType::OnPipelineStart,
        };
        n.notify(pipeline_start_event).await;
    }

    // 2. Iterate over steps
    for step in steps {
        // For logging or event purposes, let's define a step name
        let step_name = step_name(step);

        // We first validate
        let decision = step.validate(&current_event).await?;

        match decision {
            PipelineDecision::Abort(reason) => {
                // Publish OnPipelineAbort
                if let Some(n) = notifier {
                    let abort_event = NotifierEvent {
                        event_id: uuidv7::create(),
                        correlation_id: correlation_id.clone(),
                        timestamp: Utc::now(),
                        booking_id: current_event.booking_id.clone(),
                        step_name: Some(step_name.clone()),
                        event_type: NotifierEventType::OnPipelineAbort,
                    };
                    n.notify(abort_event).await;
                }

                return Err(format!("Pipeline aborted: {}", reason));
            }
            PipelineDecision::Skip => {
                // Publish OnStepSkipped
                if let Some(n) = notifier {
                    let skipped_event = NotifierEvent {
                        event_id: uuidv7::create(),
                        correlation_id: correlation_id.clone(),
                        timestamp: Utc::now(),
                        booking_id: current_event.booking_id.clone(),
                        step_name: Some(step_name.clone()),
                        event_type: NotifierEventType::OnStepSkipped,
                    };
                    n.notify(skipped_event).await;
                }

                // Do not execute the step
                continue;
            }
            PipelineDecision::Run => {
                // Publish OnStepStart
                if let Some(n) = notifier {
                    let start_event = NotifierEvent {
                        event_id: uuidv7::create(),
                        correlation_id: correlation_id.clone(),
                        timestamp: Utc::now(),
                        booking_id: current_event.booking_id.clone(),
                        step_name: Some(step_name.clone()),
                        event_type: NotifierEventType::OnStepStart,
                    };
                    n.notify(start_event).await;
                }

                // Actually run the step
                current_event = step.execute(current_event).await?;

                // Publish OnStepCompleted
                if let Some(n) = notifier {
                    let completed_event = NotifierEvent {
                        event_id: uuidv7::create(),
                        correlation_id: correlation_id.clone(),
                        timestamp: Utc::now(),
                        booking_id: current_event.booking_id.clone(),
                        step_name: Some(step_name.clone()),
                        event_type: NotifierEventType::OnStepCompleted,
                    };
                    n.notify(completed_event).await;
                }
            }
        }
    }

    // 3. If all steps succeed, publish OnPipelineEnd
    if let Some(n) = notifier {
        let end_event = NotifierEvent {
            event_id: uuidv7::create(),
            correlation_id,
            timestamp: Utc::now(),
            booking_id: current_event.booking_id.clone(),
            step_name: None,
            event_type: NotifierEventType::OnPipelineEnd,
        };
        n.notify(end_event).await;
    }

    Ok(current_event)
}

/// A helper to name each pipeline step for logging/notifier events.
fn step_name(step: &PipelineStep) -> String {
    match step {
        PipelineStep::PaymentStatus(_) => "GetPaymentStatusFromPaymentProvider".to_owned(),
        PipelineStep::BookingCall(_) => "CreateBookingCallForTravelProvider".to_owned(),
        PipelineStep::SendEmail(_) => "SendEmailNotification".to_owned(),
        PipelineStep::Mock(_) => "MockStep".to_owned(),
    }
}

// The rest of the code (steps, mock, tests, etc.) remains as before.