// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::time::Duration;

use crate::api::Sink;
use crate::codecs::Serializer;
use crate::{Error, Event};
use async_trait::async_trait;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    producer::FutureProducer,
    ClientConfig,
};
use snafu::{OptionExt, ResultExt, Snafu};
use tower::{limit::ConcurrencyLimit, Service, ServiceExt};
use tracing::{info, trace};

use super::{
    config::QUEUED_MIN_MESSAGES, request::KafkaRequestBuilder, service::KafkaService,
    KafkaSinkConfig,
};

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
pub(super) enum SinkError {
    #[snafu(display("creating kafka producer failed: {}", source))]
    CreateFailed { source: KafkaError },
    #[snafu(display("creating kafka request failed"))]
    RequestCreateFailed {},
    #[snafu(display("kafka service call failed {}", source))]
    ServiceCallFailed { source: KafkaError },
}

pub struct KafkaSink {
    serializer: Serializer,
    service: KafkaService,
    key_field: Option<String>,
}

pub(crate) fn create_producer(
    client_config: ClientConfig,
) -> crate::Result<FutureProducer> {
    let producer = client_config.create().context(CreateFailedSnafu)?;
    Ok(producer)
}

impl KafkaSink {
    pub(crate) fn new(config: KafkaSinkConfig) -> crate::Result<Self> {
        let producer_config = config.to_rdkafka()?;
        let producer = create_producer(producer_config)?;
        Ok(KafkaSink {
            service: KafkaService::new(producer, config.topic),
            serializer: config.serializer_config.build()?,
            key_field: config.key_field,
        })
    }

    async fn run_inner(self: Box<Self>, input: Event) -> Result<(), Error> {
        // rdkafka will internally retry forever, so we need some limit to prevent this from overflowing
        let mut service =
            ConcurrencyLimit::new(self.service, QUEUED_MIN_MESSAGES as usize);
        let mut req_builder = KafkaRequestBuilder {
            key_field: self.key_field,
            serializer: self.serializer,
        };

        let request = req_builder
            .build_request(input)
            .context(RequestCreateFailedSnafu)?;

        let result = service
            .ready()
            .await?
            .call(request)
            .await
            .context(ServiceCallFailedSnafu)?;
        info!("Kafka response {:?}", result);
        Ok(())
    }
}

pub(crate) async fn health_check(config: KafkaSinkConfig) -> crate::Result<()> {
    trace!("Health check started.");
    let client = config.to_rdkafka()?;
    tokio::task::spawn_blocking(move || {
        let consumer: BaseConsumer = client.create()?;
        consumer
            .fetch_metadata(Some(&config.topic), Duration::from_secs(3))
            .map(|_| ())
    })
    .await??;
    trace!("Health check completed.");
    Ok(())
}

#[async_trait]
impl Sink for KafkaSink {
    async fn run(self: Box<Self>, input: Event) -> Result<(), Error> {
        self.run_inner(input).await
    }
}
