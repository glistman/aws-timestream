use aws_credentials::errors::AwsCredentialsError;
use aws_signing_request::error::SigningError;
use reqwest::Error;

use crate::error::TimestreamErrorCause::HttpError;

#[derive(Debug)]
pub struct TimestreamError {
    pub cause: TimestreamErrorCause,
}

impl TimestreamError {
    pub fn new(cause: TimestreamErrorCause) -> TimestreamError {
        TimestreamError { cause }
    }

    pub fn from_credential_error(error: AwsCredentialsError) -> TimestreamError {
        TimestreamError {
            cause: TimestreamErrorCause::CredentialsError(error),
        }
    }

    pub fn from_request_error(error: Error) -> TimestreamError {
        TimestreamError::new(HttpError {
            code: error
                .status()
                .map(|status| status.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            response: error.to_string(),
        })
    }

    pub fn from_request_error_serialization(error: Error) -> TimestreamError {
        TimestreamError::new(HttpError {
            code: error
                .status()
                .map(|status| status.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            response: format!("Error Deserialize Json Response: {:?}", error),
        })
    }
}

#[derive(Debug)]
pub enum TimestreamErrorCause {
    HttpError { code: String, response: String },
    EmptyEndpoint,
    SigningRequest(SigningError),
    ErrorToAcquireWriteLock,
    JsonError,
    CredentialsError(AwsCredentialsError),
}
