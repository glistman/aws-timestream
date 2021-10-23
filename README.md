# AWS Timestream writer
client for aws timestream

Features:
* writer

## Usage

```bash
use std::{sync::Arc, time::Duration};

use aws_timestream::timestream::{Dimension, Record, Timestream, WriteRequest};
use chrono::Utc;
use rand::Rng;
use tokio::sync::RwLock;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let aws_access_key_id = env!("AWS_ACCESS_KEY_ID");
    let aws_secret_access_key = env!("AWS_SECRET_ACCESS_KEY");

    let timestream = Arc::new(RwLock::new(
        Timestream::new(
            "ingest".to_string(),
            "us-east-1".to_string(),
            aws_access_key_id.to_string(),
            aws_secret_access_key.to_string(),
        )
        .await
        .unwrap(),
    ));

    let refresh_timestream = timestream.clone();

    tokio::spawn(async move {
        loop {
            let timestream = refresh_timestream.read().await;
            timestream.await_to_reload().await;
            let mut timestream = refresh_timestream.write().await;
            timestream.reload_enpoints().await;
        }
    });

    loop {
        let mut rng = rand::thread_rng();
        let timestream = timestream.clone();
        let timestream = timestream.read().await;

        sleep(Duration::from_secs(1)).await;

        let time = Utc::now().timestamp_nanos();

        let write_request = WriteRequest {
            database_name: "sampleDB".to_string(),
            table_name: "test".to_string(),
            records: vec![
                Record {
                    dimensions: vec![
                        Dimension {
                            dimension_value_type: "VARCHAR".to_string(),
                            name: "microservice".to_string(),
                            value: "test-ms".to_string(),
                        },
                        Dimension {
                            dimension_value_type: "VARCHAR".to_string(),
                            name: "host".to_string(),
                            value: "192.168.10.10".to_string(),
                        },
                    ],
                    measure_name: "cpu".to_string(),
                    measure_value: rng.gen::<f64>().to_string(),
                    measure_value_type: "DOUBLE".to_string(),
                    time: time.to_string(),
                    time_unit: "NANOSECONDS".to_string(),
                    version: 1,
                },
                Record {
                    dimensions: vec![
                        Dimension {
                            dimension_value_type: "VARCHAR".to_string(),
                            name: "microservice".to_string(),
                            value: "test-ms".to_string(),
                        },
                        Dimension {
                            dimension_value_type: "VARCHAR".to_string(),
                            name: "host".to_string(),
                            value: "192.168.10.10".to_string(),
                        },
                    ],
                    measure_name: "request".to_string(),
                    measure_value: rng.gen::<u32>().to_string(),
                    measure_value_type: "BIGINT".to_string(),
                    time: time.to_string(),
                    time_unit: "NANOSECONDS".to_string(),
                    version: 1,
                },
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