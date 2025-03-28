use std::{collections::HashMap, time::Duration};

use tokio::time::sleep;
use tracing::info;

use crate::{byte_capacity::ByteCapacity, environment::Environment, environment2::Environment2, error::StreamCreateError};

/// Builder for creating a RabbitMQ stream
pub struct StreamCreator2 {
    pub(crate) env: Environment2,
    pub options: HashMap<String, String>,
}

impl StreamCreator2 {
    pub fn new(env: Environment2) -> Self {
        let creator = Self {
            env,
            options: HashMap::new(),
        };

        creator.leader_locator(LeaderLocator::LeastLeaders)
    }

    /// Create a stream with name and options
    pub async fn create(self, stream: &str) -> Result<(), StreamCreateError> {
        let client = self.env.create_client().await?;
        info!("AAA");
        let response = client.create_stream(stream, self.options).await?;
        info!("BBB {:?}", response);
        client.close().await;
        info!("client closed");

        if response.is_ok() {
            Ok(())
        } else {
            Err(StreamCreateError::Create {
                stream: stream.to_owned(),
                status: response.code().clone(),
            })
        }
    }

    pub async fn create_super_stream(
        self,
        super_stream: &str,
        number_of_partitions: usize,
        binding_keys: Option<Vec<String>>,
    ) -> Result<(), StreamCreateError> {
        let mut partitions_names = Vec::with_capacity(number_of_partitions);
        let mut new_binding_keys: Vec<String> = Vec::with_capacity(number_of_partitions);

        if binding_keys.is_none() {
            for i in 0..number_of_partitions {
                new_binding_keys.push(i.to_string());
                partitions_names.push(super_stream.to_owned() + "-" + i.to_string().as_str())
            }
        } else {
            new_binding_keys = binding_keys.unwrap();
            for binding_key in new_binding_keys.iter() {
                partitions_names.push(super_stream.to_owned() + "-" + binding_key)
            }
        }

        println!(".............");
        let client = self.env.create_client().await?;
        let response = client
            .create_super_stream(
                super_stream,
                partitions_names,
                new_binding_keys,
                self.options,
            )
            .await?;
        client.close().await?;
        println!(".............");
        // sleep(Duration::from_secs(5)).await;

        if !response.is_ok() {
            return Err(StreamCreateError::Create {
                stream: super_stream.to_owned(),
                status: response.code().clone(),
            });
        }

        Ok(())
    }

    pub fn max_age(mut self, max_age: Duration) -> Self {
        self.options
            .insert("max-age".to_owned(), format!("{}s", max_age.as_secs()));
        self
    }
    pub fn leader_locator(mut self, leader_locator: LeaderLocator) -> Self {
        self.options.insert(
            "queue-leader-locator".to_owned(),
            leader_locator.as_ref().to_string(),
        );
        self
    }
    pub fn max_length(mut self, byte_capacity: ByteCapacity) -> Self {
        self.options.insert(
            "max-length-bytes".to_owned(),
            byte_capacity.bytes().to_string(),
        );
        self
    }
    pub fn max_segment_size(mut self, byte_capacity: ByteCapacity) -> Self {
        self.options.insert(
            "stream-max-segment-size-bytes".to_owned(),
            byte_capacity.bytes().to_string(),
        );
        self
    }
}

pub enum LeaderLocator {
    ClientLocal,
    Random,
    LeastLeaders,
}

impl AsRef<str> for LeaderLocator {
    fn as_ref(&self) -> &str {
        match self {
            LeaderLocator::ClientLocal => "client-local",
            LeaderLocator::Random => "random",
            LeaderLocator::LeastLeaders => "least-leaders",
        }
    }
}
