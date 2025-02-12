// notifier_event.rs
use crate::uuidv7;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};

/// All possible notifier event types.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum NotifierEventType {
    OnStepStart,
    OnStepCompleted,
    OnStepSkipped,
    OnPipelineStart,
    OnPipelineEnd,
    OnPipelineAbort,
}

type Uuid = String;

/// A structured notifier event with metadata.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NotifierEvent {
    pub event_id: Uuid,
    pub correlation_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub booking_id: String,
    pub step_name: Option<String>, // None for pipeline-level events.
    pub event_type: NotifierEventType,
}

impl NotifierEvent {
    pub fn new_step_start(booking_id: String, step_name: String, corr_id: Uuid) -> Self {
        Self {
            event_id: uuidv7::create(),
            correlation_id: corr_id,
            timestamp: Utc::now(),
            booking_id,
            step_name: Some(step_name),
            event_type: NotifierEventType::OnStepStart,
        }
    }

    // Similarly, you can add other constructors (new_step_completed, new_pipeline_start, etc.)

    /// Construct a topic string based on the event details.
    /// Format: "booking:{booking_id}:step:{step_name}:{event_type}"
    /// If step_name is None, a placeholder (*) is used.
    pub fn topic(&self) -> String {
        let step = self.step_name.as_deref().unwrap_or("*");
        format!(
            "booking:{}:step:{}:{}",
            self.booking_id,
            step,
            format_event_type(&self.event_type)
        )
    }
}

fn format_event_type(event_type: &NotifierEventType) -> &'static str {
    match event_type {
        NotifierEventType::OnStepStart => "on_step_start",
        NotifierEventType::OnStepCompleted => "on_step_completed",
        NotifierEventType::OnStepSkipped => "on_step_skipped",
        NotifierEventType::OnPipelineStart => "on_pipeline_start",
        NotifierEventType::OnPipelineEnd => "on_pipeline_end",
        NotifierEventType::OnPipelineAbort => "on_pipeline_abort",
    }
}
