use std::sync::Arc;

use crate::{
    discovery::TimestreamDiscovery,
    error::{
        TimestreamError,
        TimestreamErrorCause::{HttpError, JsonError},
    },
};
use aws_signing_request::request::{
    CanonicalRequestBuilder, AUTHORIZATION, X_AMZ_CONTENT_SHA256, X_AMZ_DATE,
};
use aws_signing_request::request::{AWS_JSON_CONTENT_TYPE, X_AWZ_TARGET};
use chrono::Utc;
use reqwest::Response;
use serde::Serialize;
use std::time::Duration;
use tokio::time::sleep;

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
    pub dimensions: &'a Vec<&'a Dimension<'a>>,
    #[serde(rename = "MeasureName")]
    pub measure_name: &'a str,
    #[serde(rename = "MeasureValue")]
    pub measure_value: String,
    #[serde(rename = "MeasureValueType")]
    pub measure_value_type: MeasureValueType,
    #[serde(rename = "Time")]
    pub time: &'a str,
    #[serde(rename = "TimeUnit")]
    pub time_unit: TimeUnit,
    #[serde(rename = "Version")]
    pub version: u32,
}

#[derive(Serialize)]
pub struct Dimension<'a> {
    #[serde(rename = "DimensionValueType")]
    pub dimension_value_type: DimensionValueType,
    #[serde(rename = "Name")]
    pub name: &'a str,
    #[serde(rename = "Value")]
    pub value: &'a str,
}

#[derive(Debug)]
pub struct Timestream {
    discovery: TimestreamDiscovery,
    reload_error: bool,
    aws_access_key_id: String,
    aws_secret_access_key: String,
    region: String,
}

impl Timestream {
    pub async fn new(
        action: String,
        region: String,
        aws_access_key_id: String,
        aws_secret_access_key: String,
    ) -> Result<Timestream, TimestreamError> {
        let mut discovery = TimestreamDiscovery::new(
            action,
            region.clone(),
            aws_access_key_id.clone(),
            aws_secret_access_key.clone(),
        );
        discovery.reload_enpoints().await?;

        Ok(Timestream {
            discovery,
            reload_error: false,
            aws_access_key_id,
            aws_secret_access_key,
            region,
        })
    }

    pub async fn await_to_reload(&self) {
        if self.reload_error {
            sleep(Duration::from_secs(1)).await;
        } else {
            sleep(Duration::from_secs(
                self.discovery.min_cache_period_in_minutes * 60,
            ))
            .await;
        }
    }

    pub async fn reload_enpoints(&mut self) {
        match self.discovery.reload_enpoints().await {
            Ok(_) => self.reload_error = false,
            Err(_) => self.reload_error = true,
        }
    }

    pub async fn get_enpoint<'a>(&'a self) -> Result<&'a str, TimestreamError> {
        self.discovery.get_next_enpoint()
    }

    pub async fn write<'a>(
        &self,
        write_request: WriteRequest<'a>,
    ) -> Result<Response, TimestreamError> {
        let client = reqwest::Client::new();
        let enpoint = self.get_enpoint().await?;
        let url = format!("https://{}", enpoint);
        let body =
            serde_json::to_string(&write_request).map_err(|_| TimestreamError::new(JsonError))?;

        let mut canonical_request_builder = CanonicalRequestBuilder::new(
            &enpoint,
            "POST",
            "/",
            &self.aws_access_key_id,
            &self.aws_secret_access_key,
            &self.region,
            "timestream",
        );

        let canonical_request = canonical_request_builder
            .header("Content-Type", AWS_JSON_CONTENT_TYPE)
            .header(X_AWZ_TARGET, "Timestream_20181101.WriteRecords")
            .body(&body)
            .build(Utc::now());

        client
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
            .body(body)
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
            })
    }
}
