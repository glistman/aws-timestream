# AWS Timestream writer
client for aws timestream

Features:
* writer

## Usage

```bash
use std::time::Duration;

use aws_credentials::basic_credentials::AwsBasicCretendtialsProvider;
use aws_timestream::timestream::DimensionValueType::VARCHAR;
use aws_timestream::timestream::MeasureValueType::{BIGINT, DOUBLE};
use aws_timestream::timestream::TimeUnit::NANOSECONDS;
use aws_timestream::timestream::{Dimension, Record, Timestream, WriteRequest};
use chrono::Utc;
use rand::Rng;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let aws_access_key_id = env!("AWS_ACCESS_KEY_ID");
    let aws_secret_access_key = env!("AWS_SECRET_ACCESS_KEY");

    let aws_credentials_provider =
        AwsBasicCretendtialsProvider::new(aws_access_key_id, aws_secret_access_key);

    let timestream = Timestream::new("ingest", "us-east-1", aws_credentials_provider.clone()).await;

    let dimensions = vec![
        &Dimension {
            dimension_value_type: VARCHAR,
            name: "microservice",
            value: "test-ms",
        },
        &Dimension {
            dimension_value_type: VARCHAR,
            name: "host",
            value: "192.168.10.10",
        },
    ];

    loop {
        let mut rng = rand::thread_rng();
        let timestream = timestream.clone();
        let timestream = timestream.read().await;

        sleep(Duration::from_secs(1)).await;

        let time = Utc::now().timestamp_nanos();
        let time = time.to_string();

        let write_request = WriteRequest {
            database_name: "sampleDB",
            table_name: "prueba",
            records: vec![
                Record::new(
                    &dimensions,
                    "cpu",
                    rng.gen::<f64>().to_string(),
                    DOUBLE,
                    &time,
                    NANOSECONDS,
                    1,
                ),
                Record::new(
                    &dimensions,
                    "request",
                    rng.gen::<u32>().to_string(),
                    BIGINT,
                    &time,
                    NANOSECONDS,
                    1,
                ),
            ],
        };

        let response = timestream.write(write_request).await;
        println!("{:?}", response);
    }
}
```





## Usage With Container Credentials

```bash
use std::time::Duration;

use aws_credentials::container_credentials::AwsContrainerCretendtialsProvider;
use aws_timestream::timestream::DimensionValueType::VARCHAR;
use aws_timestream::timestream::MeasureValueType::{BIGINT, DOUBLE};
use aws_timestream::timestream::TimeUnit::NANOSECONDS;
use aws_timestream::timestream::{Dimension, Record, Timestream, WriteRequest};
use chrono::Utc;
use rand::Rng;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let aws_credentials_provider = AwsContrainerCretendtialsProvider::new().await;
    let timestream = Timestream::new("ingest", "us-east-1", aws_credentials_provider.clone()).await;

    let dimensions = vec![
        &Dimension {
            dimension_value_type: VARCHAR,
            name: "microservice",
            value: "test-ms",
        },
        &Dimension {
            dimension_value_type: VARCHAR,
            name: "host",
            value: "192.168.10.10",
        },
    ];

    loop {
        let mut rng = rand::thread_rng();
        let timestream = timestream.clone();
        let timestream = timestream.read().await;

        sleep(Duration::from_secs(1)).await;

        let time = Utc::now().timestamp_nanos();
        let time = time.to_string();

        let write_request = WriteRequest {
            database_name: "sampleDB",
            table_name: "prueba",
            records: vec![
                Record::new(
                    &dimensions,
                    "cpu",
                    rng.gen::<f64>().to_string(),
                    DOUBLE,
                    &time,
                    NANOSECONDS,
                    1,
                ),
                Record::new(
                    &dimensions,
                    "request",
                    rng.gen::<u32>().to_string(),
                    BIGINT,
                    &time,
                    NANOSECONDS,
                    1,
                ),
            ],
        };

        let response = timestream.write(write_request).await;
        println!("{:?}", response);
    }
}
```

## Usage
[Full example](https://github.com/glistman/aws-timestream-example)

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)