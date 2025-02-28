use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// --------------------------
// Data Structures & Enums
// --------------------------

#[derive(Debug, Clone)]
pub struct ServerSideBookingEvent {
    pub payment_id: Option<String>,
    pub booking_id: String,
    pub user_email: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PipelineDecision {
    Run,
    Skip,
    Abort(String),
}

// --------------------------
// Traits
// --------------------------

/// Validation always takes &self.
#[async_trait]
pub trait PipelineValidator: Send + Sync {
    async fn validate(&self, event: &ServerSideBookingEvent) -> Result<PipelineDecision, String>;
}

/// Execution is stateless so it does not need &self.
#[async_trait]
pub trait PipelineExecutor: Send + Sync {
    async fn execute(event: ServerSideBookingEvent) -> Result<ServerSideBookingEvent, String>;
}

// --------------------------
// Concrete Steps
// --------------------------

#[derive(Debug, Clone)]
pub struct GetPaymentStatusFromPaymentProvider;

#[async_trait]
impl PipelineValidator for GetPaymentStatusFromPaymentProvider {
    async fn validate(&self, event: &ServerSideBookingEvent) -> Result<PipelineDecision, String> {
        if event.payment_id.is_some() {
            Ok(PipelineDecision::Run)
        } else {
            Ok(PipelineDecision::Skip)
        }
    }
}

#[async_trait]
impl PipelineExecutor for GetPaymentStatusFromPaymentProvider {
    async fn execute(event: ServerSideBookingEvent) -> Result<ServerSideBookingEvent, String> {
        println!("Executing GetPaymentStatusFromPaymentProvider");
        Ok(event)
    }
}

#[derive(Debug, Clone)]
pub struct CreateBookingCallForTravelProvider;

#[async_trait]
impl PipelineValidator for CreateBookingCallForTravelProvider {
    async fn validate(&self, event: &ServerSideBookingEvent) -> Result<PipelineDecision, String> {
        if event.booking_id.is_empty() {
            Ok(PipelineDecision::Abort("Missing booking_id".into()))
        } else {
            Ok(PipelineDecision::Run)
        }
    }
}

#[async_trait]
impl PipelineExecutor for CreateBookingCallForTravelProvider {
    async fn execute(event: ServerSideBookingEvent) -> Result<ServerSideBookingEvent, String> {
        println!("Executing CreateBookingCallForTravelProvider");
        Ok(event)
    }
}

#[derive(Debug, Clone)]
pub struct SendEmailNotification;

#[async_trait]
impl PipelineValidator for SendEmailNotification {
    async fn validate(&self, event: &ServerSideBookingEvent) -> Result<PipelineDecision, String> {
        if event.user_email.is_empty() {
            Ok(PipelineDecision::Abort("Missing user email".into()))
        } else {
            Ok(PipelineDecision::Run)
        }
    }
}

#[async_trait]
impl PipelineExecutor for SendEmailNotification {
    async fn execute(event: ServerSideBookingEvent) -> Result<ServerSideBookingEvent, String> {
        println!("Executing SendEmailNotification to: {}", event.user_email);
        // In a real implementation, this would send an actual email
        Ok(event)
    }
}

// --------------------------
// Mock Step for Testing
// --------------------------

#[derive(Debug, Clone)]
pub struct MockStep {
    pub decision: PipelineDecision,
    pub executed: Arc<AtomicBool>,
}

#[async_trait]
impl PipelineValidator for MockStep {
    async fn validate(&self, _event: &ServerSideBookingEvent) -> Result<PipelineDecision, String> {
        Ok(self.decision.clone())
    }
}

// (For testing, we handle execution in the enum; see below.)

// --------------------------
// PipelineStep Enum Wrapper
// --------------------------

#[derive(Debug, Clone)]
pub enum PipelineStep {
    PaymentStatus(GetPaymentStatusFromPaymentProvider),
    BookingCall(CreateBookingCallForTravelProvider),
    SendEmail(SendEmailNotification),
    Mock(MockStep),
}

impl PipelineStep {
    /// Delegates validation to the inner type.
    pub async fn validate(
        &self,
        event: &ServerSideBookingEvent,
    ) -> Result<PipelineDecision, String> {
        match self {
            PipelineStep::PaymentStatus(step) => step.validate(event).await,
            PipelineStep::BookingCall(step) => step.validate(event).await,
            PipelineStep::SendEmail(step) => step.validate(event).await,
            PipelineStep::Mock(step) => step.validate(event).await,
        }
    }

    /// For execution, we call the static execute function (ignoring any internal state)
    /// except for the Mock step where we want to record that execution was attempted.
    pub async fn execute(
        &self,
        event: ServerSideBookingEvent,
    ) -> Result<ServerSideBookingEvent, String> {
        match self {
            PipelineStep::PaymentStatus(_) => {
                GetPaymentStatusFromPaymentProvider::execute(event).await
            }
            PipelineStep::BookingCall(_) => {
                CreateBookingCallForTravelProvider::execute(event).await
            }
            PipelineStep::SendEmail(_) => {
                SendEmailNotification::execute(event).await
            }
            PipelineStep::Mock(step) => {
                step.executed.store(true, Ordering::SeqCst);
                Ok(event)
            }
        }
    }
}