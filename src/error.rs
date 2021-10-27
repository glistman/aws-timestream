use aws_credentials::errors::AwsCredentialsError;
use aws_signing_request::error::SigningError;

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
}

#[derive(Debug)]
pub enum TimestreamErrorCause {
    HttpError { code: String, response: String },
    EmptyEnpoint,
    SigninRequest(SigningError),
    ErrorToAdquireWriteLock,
    JsonError,
    CredentialsError(AwsCredentialsError),
}
