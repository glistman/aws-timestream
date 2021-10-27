use std::sync::{atomic::AtomicUsize, Arc};

use aws_credentials::credential_provider::AwsCredentialProvider;
use aws_signing_request::request::{
    CanonicalRequestBuilder, AUTHORIZATION, AWS_JSON_CONTENT_TYPE, X_AMZ_CONTENT_SHA256,
    X_AMZ_DATE, X_AMZ_SECURITY_TOKEN, X_AWZ_TARGET,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{
    TimestreamError,
    TimestreamErrorCause::{EmptyEnpoint, HttpError, SigninRequest},
};

use std::sync::atomic::Ordering;

pub struct TimestreamDiscovery {
    host: String,
    url: String,
    region: String,
    credential_provider: Arc<RwLock<dyn AwsCredentialProvider + Sync + Send>>,
    enpoints: Vec<TimestreamEnpoint>,
    enpoint_index: AtomicUsize,
    pub min_cache_period_in_minutes: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TimestreamEnpointResponse {
    #[serde(alias = "Endpoints")]
    pub endponts: Vec<TimestreamEnpoint>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TimestreamEnpoint {
    #[serde(alias = "Address")]
    pub address: String,
    #[serde(alias = "CachePeriodInMinutes")]
    pub cache_period_in_minutes: u64,
}

impl<'a> TimestreamDiscovery {
    pub fn new(
        action: String,
        region: String,
        credential_provider: Arc<RwLock<dyn AwsCredentialProvider + Sync + Send>>,
    ) -> TimestreamDiscovery {
        let host = format!("{}.timestream.{}.amazonaws.com", &action, &region);
        let url = format!("https://{}", host);

        TimestreamDiscovery {
            host,
            url,
            region,
            credential_provider,
            enpoints: Vec::new(),
            enpoint_index: AtomicUsize::new(0),
            min_cache_period_in_minutes: 60,
        }
    }

    pub async fn reload_enpoints(&mut self) -> Result<(), TimestreamError> {
        let enpoints_response = self.query_enpoints().await?;
        self.enpoint_index
            .store(enpoints_response.len() - 1, Ordering::SeqCst);

        let min_ttl = enpoints_response
            .iter()
            .map(|enpoint| enpoint.cache_period_in_minutes)
            .min()
            .unwrap_or(0);

        println!("Reload:{:?}", &enpoints_response);
        self.enpoints = enpoints_response;
        self.min_cache_period_in_minutes = min_ttl;

        Ok(())
    }

    async fn query_enpoints(&mut self) -> Result<Vec<TimestreamEnpoint>, TimestreamError> {
        let credentials_provider = self.credential_provider.read().await;
        let credentials = credentials_provider
            .get_credentials()
            .await
            .map_err(TimestreamError::from_credential_error)?;

        let body = "{}";

        let mut canonical_request_builder = CanonicalRequestBuilder::new(
            self.host.as_str(),
            "POST",
            "/",
            &credentials.aws_access_key_id,
            &credentials.aws_secret_access_key,
            &self.region,
            "timestream",
        );

        let canonical_request = canonical_request_builder
            .header(X_AWZ_TARGET, "Timestream_20181101.DescribeEndpoints")
            .header_opt_ref(X_AMZ_SECURITY_TOKEN, &credentials.aws_session_token)
            .body(body)
            .build(Utc::now());

        let authorization = canonical_request
            .calculate_authorization()
            .map_err(|error| TimestreamError::new(SigninRequest(error)))?;

        let client = reqwest::Client::new();

        let request = client
            .post(&self.url)
            .header(X_AMZ_DATE, &canonical_request.date.iso_8601)
            .header("Content-Type", AWS_JSON_CONTENT_TYPE)
            .header(X_AWZ_TARGET, "Timestream_20181101.DescribeEndpoints")
            .header(X_AMZ_CONTENT_SHA256, &canonical_request.content_sha_256)
            .header(AUTHORIZATION, &authorization)
            .body(body);

        let request = if let Some(token) = &credentials.aws_session_token {
            request.header(X_AMZ_SECURITY_TOKEN, token)
        } else {
            request
        };

        request
            .send()
            .await
            .map_err(|error| {
                TimestreamError::new(HttpError {
                    code: error
                        .status()
                        .map(|status| status.to_string())
                        .unwrap_or("unknown".to_string()),
                    response: error.to_string(),
                })
            })?
            .json::<TimestreamEnpointResponse>()
            .await
            .map_err(|error| {
                TimestreamError::new(HttpError {
                    code: error
                        .status()
                        .map(|status| status.to_string())
                        .unwrap_or("unknown".to_string()),
                    response: "Error Deserialize Json Response".to_string(),
                })
            })
            .map(|response| response.endponts)
    }

    pub fn get_next_enpoint(&'a self) -> Result<&'a str, TimestreamError> {
        let max_enpoint_index = self.enpoints.len() - 1;

        let index = if let Ok(_) = self.enpoint_index.compare_exchange(
            usize::max_value(),
            max_enpoint_index,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            max_enpoint_index
        } else {
            self.enpoint_index.fetch_sub(1, Ordering::Acquire)
        };

        match self.enpoints.get(index) {
            Some(enpoint) => Ok(&enpoint.address),
            None => Err(TimestreamError::new(EmptyEnpoint)),
        }
    }
}
