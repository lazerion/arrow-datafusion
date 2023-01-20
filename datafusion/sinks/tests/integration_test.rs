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

use std::{num::NonZeroU64, time::Duration};

use datafusion_sinks::api::SinkConfig;
use datafusion_sinks::codecs::json::JsonSerializerConfig;
use datafusion_sinks::codecs::SerializerConfig;
use datafusion_sinks::kafka::KafkaSinkConfig;
use datafusion_sinks::{AggregationData, Event};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    Offset, TopicPartitionList,
};

#[tokio::test]
#[ignore]
async fn test_health_check() {
    let topic = format!("{}-{}", random_string(5), random_string(5));
    let config = KafkaSinkConfig::default()
        .with_bootstrap_servers("localhost:9092")
        .with_topic(topic);

    let hc = config.health_check().await;
    awaitility::at_most(Duration::from_millis(1000)).until(|| match hc {
        Ok(_) => true,
        Err(_) => false,
    });
}

#[tokio::test]
#[ignore]
async fn test_kafka_sink_send() {
    let topic = format!("{}-{}", random_string(5), random_string(5));
    let config = KafkaSinkConfig::default()
        .with_bootstrap_servers("localhost:9092")
        .with_topic(topic)
        .with_serializer_config(SerializerConfig::Json(JsonSerializerConfig {}))
        .with_message_timeout(5000);
    let sink = config.build().unwrap();

    let data = AggregationData::new().with_interval_ms(NonZeroU64::new(42));
    let event = Event::Aggregation(data);
    let result = sink.run(event).await;
    assert!(result.is_ok());

    let consumer = consumer(&config);

    awaitility::at_most(Duration::from_millis(1000)).until(|| {
        match consumer.fetch_watermarks(&config.topic, 0, Duration::from_secs(3)) {
            Ok((low, high)) => high == 1 && low == 0,
            Err(err) => {
                println!("retrying due to error fetching watermarks: {}", err);
                false
            }
        }
    });
}

fn consumer(config: &KafkaSinkConfig) -> BaseConsumer {
    // read back everything from the beginning
    let mut client_config = rdkafka::ClientConfig::new();
    client_config.set("bootstrap.servers", &config.bootstrap_servers);
    client_config.set("enable.partition.eof", "true");
    client_config.set("group.id", &random_string(10));

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(&config.topic, 0)
        .set_offset(Offset::Beginning)
        .unwrap();

    let consumer: BaseConsumer = client_config.create().unwrap();
    consumer.assign(&tpl).unwrap();
    consumer
}

fn random_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
}
