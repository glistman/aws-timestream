use std::sync::Arc;
use std::time::Duration;

use aws_credentials::credential_provider::AwsCredentialProvider;
use aws_signing_request::request::{
    CanonicalRequestBuilder, AUTHORIZATION, X_AMZ_CONTENT_SHA256, X_AMZ_DATE,
};
use aws_signing_request::request::{AWS_JSON_CONTENT_TYPE, X_AMZ_SECURITY_TOKEN, X_AWZ_TARGET};
use chrono::Utc;
use log::info;
use reqwest::{Client, Response};
use serde::Serialize;
use tokio::{sync::RwLock, time::sleep};

use crate::{
    discovery::TimestreamDiscovery,
    error::{TimestreamError, TimestreamErrorCause::JsonError},
};

#[derive(Serialize)]
pub enum MeasureValueType {
    DOUBLE,
    BIGINT,
    VARCHAR,
    BOOLEAN,
}

#[derive(Serialize)]
pub enum DimensionValueType {
    VARCHAR,
}

#[derive(Serialize)]
pub enum TimeUnit {
    MILLISECONDS,
    SECONDS,
    MICROSECONDS,
    NANOSECONDS,
}

#[derive(Serialize)]
pub struct WriteRequest<'a> {
    #[serde(rename = "DatabaseName")]
    pub database_name: &'a str,
    #[serde(rename = "TableName")]
    pub table_name: &'a str,
    #[serde(rename = "Records")]
    pub records: Vec<Record<'a>>,
}

#[derive(Serialize)]
pub struct Record<'a> {
    #[serde(rename = "Dimensions")]
    pub dimensions: &'a [Dimension],
    #[serde(rename = "MeasureName")]
    pub measure_name: String,
    #[serde(rename = "MeasureValue")]
    pub measure_value: String,
    #[serde(rename = "MeasureValueType")]
    pub measure_value_type: MeasureValueType,
    #[serde(rename = "Time")]
    pub time: String,
    #[serde(rename = "TimeUnit")]
    pub time_unit: TimeUnit,
    #[serde(rename = "Version")]
    pub version: u32,
}

impl<'a> Record<'a> {
    pub fn new(
        dimensions: &'a [Dimension],
        measure_name: &str,
        measure_value: String,
        measure_value_type: MeasureValueType,
        time: &str,
        time_unit: TimeUnit,
        version: u32,
    ) -> Record<'a> {
        Record {
            dimensions,
            measure_name: measure_name.to_string(),
            measure_value,
            measure_value_type,
            time: time.to_string(),
            time_unit,
            version,
        }
    }
}

#[derive(Serialize)]
pub struct Dimension {
    #[serde(rename = "DimensionValueType")]
    pub dimension_value_type: DimensionValueType,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Value")]
    pub value: String,
}

pub struct Timestream {
    client: Client,
    discovery: TimestreamDiscovery,
    reload_error: bool,
    credential_provider: Arc<RwLock<dyn AwsCredentialProvider + Sync + Send>>,
    region: String,
}

impl Timestream {
    pub async fn new(
        action: &str,
        region: &str,
        credential_provider: Arc<RwLock<dyn AwsCredentialProvider + Sync + Send>>,
    ) -> Arc<RwLock<Timestream>> {
        let mut discovery = TimestreamDiscovery::new(
            action.to_string(),
            region.to_string(),
            credential_provider.clone(),
        );
        let initial_endpoint = discovery.reload_endpoints().await;
        info!("initial endpoint discovery result:{:?}", initial_endpoint);

        let timestream = Arc::new(RwLock::new(Timestream {
            client: Client::new(),
            discovery,
            reload_error: false,
            credential_provider,
            region: region.to_string(),
        }));

        let refresh_timestream = timestream.clone();
        tokio::spawn(async move {
            Timestream::execute_refresh_endpoint_procedure(refresh_timestream).await;
        });

        timestream
    }

    pub fn time_to_await(&self) -> Duration {
        if self.reload_error {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(self.discovery.min_cache_period_in_minutes * 60)
        }
    }

    pub async fn reload_endpoints(&mut self) {
        match self.discovery.reload_endpoints().await {
            Ok(_) => self.reload_error = false,
            Err(_) => self.reload_error = true,
        }
    }

    pub async fn get_endpoint(&self) -> Result<&str, TimestreamError> {
        self.discovery.get_next_endpoint()
    }

    pub async fn write<'a>(
        &'a self,
        write_request: WriteRequest<'a>,
    ) -> Result<Response, TimestreamError> {
        let endpoint = self.get_endpoint().await?;
        let url = format!("https://{}", endpoint);
        let body =
            serde_json::to_string(&write_request).map_err(|_| TimestreamError::new(JsonError))?;

        let credentials_provider = self.credential_provider.read().await;
        let credentials = credentials_provider
            .get_credentials()
            .await
            .map_err(TimestreamError::from_credential_error)?;

        let canonical_request = CanonicalRequestBuilder::new(
            endpoint,
            "POST",
            "/",
            &credentials.aws_access_key_id,
            &credentials.aws_secret_access_key,
            &self.region,
            "timestream",
        )
        .header("Content-Type", AWS_JSON_CONTENT_TYPE)
        .header(X_AWZ_TARGET, "Timestream_20181101.WriteRecords")
        .header_opt_ref(X_AMZ_SECURITY_TOKEN, &credentials.aws_session_token)
        .body(&body)
        .build(Utc::now());

        let request = self
            .client
            .post(url)
            .header(X_AMZ_DATE, &canonical_request.date.iso_8601)
            .header("Content-Type", AWS_JSON_CONTENT_TYPE)
            .header(X_AWZ_TARGET, "Timestream_20181101.WriteRecords")
            .header(X_AMZ_CONTENT_SHA256, &canonical_request.content_sha_256)
            .header(
                AUTHORIZATION,
                &canonical_request
                    .calculate_authorization()
                    .expect("Authorization creation failed"),
            )
            .body(body);

        let request = if let Some(token) = &credentials.aws_session_token {
            request.header(X_AMZ_SECURITY_TOKEN, token)
        } else {
            request
        };

        request
            .send()
            .await
            .map_err(TimestreamError::from_request_error)
    }

    pub async fn execute_refresh_endpoint_procedure(refresh_timestream: Arc<RwLock<Timestream>>) {
        loop {
            info!("timestream:reload");
            let timestream = refresh_timestream.read().await;
            let time_to_await = timestream.time_to_await();
            drop(timestream);
            sleep(time_to_await).await;
            let mut timestream = refresh_timestream.write().await;
            timestream.reload_endpoints().await;
        }
    }
}
