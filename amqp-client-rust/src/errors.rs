use amqprs::error::Error as AmqprsError;
use std::error::Error as StdError;
use std::fmt::{self, Display};
use tokio::sync::oneshot::error::RecvError;
use tokio::time::error::Elapsed;

#[derive(Debug)]
pub enum AppErrorType {
    InternalError,
    RpcTimeout,
    TimeoutError,
    UnexpectedResultError,
}

#[derive(Debug)]
pub struct AppError {
    pub message: Option<String>,
    pub description: Option<String>,
    pub error_type: AppErrorType,
}
impl Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, r"\{{ {:?}, {:?})\}}", self.message, self.description)
    }
}
impl AppError {
    pub fn new(
        message: Option<String>,
        description: Option<String>,
        error_type: AppErrorType,
    ) -> AppError {
        AppError {
            message,
            description,
            error_type,
        }
    }
    pub fn get_message(&self) -> String {
        match self {
            AppError {
                error_type: AppErrorType::RpcTimeout,
                ..
            } => "The timeout for rpc call was reached".to_string(),
            AppError {
                error_type: AppErrorType::TimeoutError,
                ..
            } => "Timeout: failed to connect, order rejected...".to_string(),
            AppError {
                error_type: AppErrorType::UnexpectedResultError,
                ..
            } => "Unexpected result from eventbus operation".to_string(),
            
            AppError {
                error_type: AppErrorType::InternalError,
                ..
            } => "An unexpected error has occurred".to_string(),
        }
    }
}

impl From<Box<dyn StdError>> for AppError {
    fn from(error: Box<dyn StdError>) -> AppError {
        println!("{:?}", error);
        AppError {
            message: None,
            description: Some(error.to_string()),
            error_type: AppErrorType::InternalError,
        }
    }
}

impl From<AmqprsError> for AppError {
    fn from(value: AmqprsError) -> Self {
        AppError {
            message: None,
            description: Some(value.to_string()),
            error_type: AppErrorType::InternalError,
        }
    }
}

impl StdError for AppError {
    // The `source` method is optional. If your error type doesn't wrap another error,
    // you can simply return `None`.
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl From<RecvError> for AppError {
    fn from(error: RecvError) -> Self {
        AppError {
            message: None,
            description: Some(error.to_string()),
            error_type: AppErrorType::InternalError,
        }
    }
}
impl From<Elapsed> for AppError {
    fn from(error: Elapsed) -> Self {
        AppError {
            message: None,
            description: Some(error.to_string()),
            error_type: AppErrorType::RpcTimeout,
        }
    }
}
