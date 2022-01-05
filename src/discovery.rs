use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicUsize, Arc};

use aws_credentials::credential_provider::{AwsCredentialProvider, AwsCredentials};
use aws_signing_request::request::{
    CanonicalRequest, CanonicalRequestBuilder, AUTHORIZATION, AWS_JSON_CONTENT_TYPE,
    X_AMZ_CONTENT_SHA256, X_AMZ_DATE, X_AMZ_SECURITY_TOKEN, X_AWZ_TARGET,
};
use chrono::Utc;
use log::{debug, info};
use reqwest::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{
    TimestreamError,
    TimestreamErrorCause::{EmptyEndpoint, SigningRequest},
};

pub struct TimestreamDiscovery {
    client: Client,
    host: String,
    url: String,
    region: String,
    credential_provider: Arc<RwLock<dyn AwsCredentialProvider + Sync + Send>>,
    endpoints: Vec<TimestreamEndpoint>,
    endpoint_index: AtomicUsize,
    pub min_cache_period_in_minutes: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TimestreamEndpointResponse {
    #[serde(alias = "Endpoints")]
    pub endpoints: Vec<TimestreamEndpoint>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TimestreamEndpoint {
    #[serde(alias = "Address")]
    pub address: String,
    #[serde(alias = "CachePeriodInMinutes")]
    pub cache_period_in_minutes: u64,
}

impl TimestreamDiscovery {
    pub fn new(
        action: String,
        region: String,
        credential_provider: Arc<RwLock<dyn AwsCredentialProvider + Sync + Send>>,
    ) -> TimestreamDiscovery {
        let host = format!("{}.timestream.{}.amazonaws.com", &action, &region);
        let url = format!("https://{}", host);

        TimestreamDiscovery {
            client: reqwest::Client::new(),
            host,
            url,
            region,
            credential_provider,
            endpoints: Vec::new(),
            endpoint_index: AtomicUsize::new(0),
            min_cache_period_in_minutes: 60,
        }
    }

    pub async fn reload_endpoints(&mut self) -> Result<bool, TimestreamError> {
        let endpoints_response = self.query_endpoints().await?;
        if endpoints_response.is_empty() {
            Err(TimestreamError::new(EmptyEndpoint))
        } else {
            self.endpoint_index
                .store(endpoints_response.len() - 1, Ordering::SeqCst);

            let min_ttl = endpoints_response
                .iter()
                .map(|endpoint| endpoint.cache_period_in_minutes)
                .min()
                .unwrap_or(0);

            info!("Reload:{:?}", &endpoints_response);
            self.endpoints = endpoints_response;
            self.min_cache_period_in_minutes = min_ttl;
            Ok(true)
        }
    }

    async fn query_endpoints(&mut self) -> Result<Vec<TimestreamEndpoint>, TimestreamError> {
        self.create_describe_endpoints_request()
            .await?
            .send()
            .await
            .map_err(TimestreamError::from_request_error)?
            .json::<TimestreamEndpointResponse>()
            .await
            .map_err(TimestreamError::from_request_error_serialization)
            .map(|response| response.endpoints)
    }

    async fn create_describe_endpoints_request(&self) -> Result<RequestBuilder, TimestreamError> {
        let body = "{}";
        let credentials_provider = self.credential_provider.read().await;
        let credentials = credentials_provider
            .get_credentials()
            .await
            .map_err(TimestreamError::from_credential_error)?;

        let canonical_request = self.create_canonical_request(body, credentials);
        let authorization = canonical_request
            .calculate_authorization()
            .map_err(|error| TimestreamError::new(SigningRequest(error)))?;

        let request = self
            .client
            .post(&self.url)
            .header(X_AMZ_DATE, &canonical_request.date.iso_8601)
            .header("Content-Type", AWS_JSON_CONTENT_TYPE)
            .header(X_AWZ_TARGET, "Timestream_20181101.DescribeEndpoints")
            .header(X_AMZ_CONTENT_SHA256, &canonical_request.content_sha_256)
            .header(AUTHORIZATION, &authorization)
            .body(body);

        if let Some(token) = &credentials.aws_session_token {
            Ok(request.header(X_AMZ_SECURITY_TOKEN, token))
        } else {
            Ok(request)
        }
    }

    fn create_canonical_request(
        &self,
        body: &str,
        credentials: &AwsCredentials,
    ) -> CanonicalRequest {
        CanonicalRequestBuilder::new(
            self.host.as_str(),
            "POST",
            "/",
            &credentials.aws_access_key_id,
            &credentials.aws_secret_access_key,
            &self.region,
            "timestream",
        )
        .header(X_AWZ_TARGET, "Timestream_20181101.DescribeEndpoints")
        .header_opt_ref(X_AMZ_SECURITY_TOKEN, &credentials.aws_session_token)
        .body(body)
        .build(Utc::now())
    }

    pub fn get_next_endpoint(&self) -> Result<&str, TimestreamError> {
        TimestreamDiscovery::next_endpoint(&self.endpoints, &self.endpoint_index)
    }

    fn next_endpoint<'a>(
        endpoints: &'a [TimestreamEndpoint],
        endpoint_index: &'a AtomicUsize,
    ) -> Result<&'a str, TimestreamError> {
        if endpoints.is_empty() {
            Err(TimestreamError::new(EmptyEndpoint))
        } else {
            let max_endpoint_index = endpoints.len() - 1;
            let reset_index = endpoint_index
                .compare_exchange(
                    usize::max_value(),
                    max_endpoint_index,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok();
            debug!("Reset Index:{}", reset_index);
            let index = endpoint_index.fetch_sub(1, Ordering::Acquire);

            match endpoints.get(index) {
                Some(endpoint) => Ok(&endpoint.address),
                None => Err(TimestreamError::new(EmptyEndpoint)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::discovery::{TimestreamDiscovery, TimestreamEndpoint};
    use log::info;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test_log::test]
    fn next_endpoint_negative() {
        let endpoint_index = AtomicUsize::new(0);
        endpoint_index.fetch_sub(1, Ordering::Acquire);
        let endpoints = &[TimestreamEndpoint {
            address: "http://localhost".to_owned(),
            cache_period_in_minutes: 3600,
        }];
        info!("endpoint_index:{:?}", endpoint_index);
        let next_endpoint = TimestreamDiscovery::next_endpoint(endpoints, &endpoint_index).unwrap();

        assert_eq!(next_endpoint, "http://localhost");
    }

    #[test_log::test]
    fn endpoint_cycle() {
        let endpoints = &[
            TimestreamEndpoint {
                address: "http://localhost:9000".to_owned(),
                cache_period_in_minutes: 3600,
            },
            TimestreamEndpoint {
                address: "http://localhost:9001".to_owned(),
                cache_period_in_minutes: 3600,
            },
        ];

        let endpoint_index = AtomicUsize::new(endpoints.len() - 1);
        info!("endpoint_index:{:?}", endpoint_index);

        for cycle in 0..13 {
            for index in (0..endpoints.len()).rev() {
                let next_endpoint =
                    TimestreamDiscovery::next_endpoint(endpoints, &endpoint_index).unwrap();
                info!(
                    "cycle:{}, index: {}, endpoint: {}",
                    cycle, index, next_endpoint
                );
                assert_eq!(next_endpoint, &format!("http://localhost:900{}", index));
            }
        }
    }
}
