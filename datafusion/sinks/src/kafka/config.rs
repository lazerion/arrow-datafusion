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

use std::collections::HashMap;

use crate::api::{HealthCheck, Sink, SinkConfig};
use crate::codecs::json::JsonSerializerConfig;
use crate::codecs::SerializerConfig;
use futures::FutureExt;
use rdkafka::ClientConfig;
use tracing::debug;

use super::sink::{health_check, KafkaSink};

const fn default_socket_timeout_ms() -> u64 {
    60000 // default in librdkafka
}

const fn default_message_timeout_ms() -> u64 {
    300000 // default in librdkafka
}

pub(crate) const QUEUED_MIN_MESSAGES: u64 = 100000;

#[derive(Clone, Debug)]
pub struct KafkaSinkConfig {
    /// Each value must be in the form of `<host>` or `<host>:<port>`, and separated by a comma.
    pub bootstrap_servers: String,
    /// The Kafka topic name to write events to.
    pub topic: String,
    /// Kafka uses a hash of the key to choose the partition or uses round-robin if the record has no key.
    pub key_field: Option<String>,
    /// Default timeout, in milliseconds, for network requests.
    pub socket_timeout_ms: u64,
    /// Local message timeout, in milliseconds.
    pub message_timeout_ms: u64,
    /// [config_props_docs]: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    pub librdkafka_options: HashMap<String, String>,
    // Serializer configuration for payload generation
    pub serializer_config: SerializerConfig,
}

impl KafkaSinkConfig {
    pub(crate) fn to_rdkafka(&self) -> crate::Result<ClientConfig> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("socket.timeout.ms", &self.socket_timeout_ms.to_string())
            .set("message.timeout.ms", &self.message_timeout_ms.to_string());

        for (key, value) in self.librdkafka_options.iter() {
            debug!(option = %key, value = %value, "Setting librdkafka option.");
            client_config.set(key.as_str(), value.as_str());
        }

        Ok(client_config)
    }

    pub fn with_topic<T: AsRef<str>>(mut self, topic: T) -> Self {
        self.topic = topic.as_ref().to_string();
        self
    }
    pub fn with_key_field<T: AsRef<str>>(mut self, key_field: T) -> Self {
        self.key_field = Some(key_field.as_ref().to_string());
        self
    }

    pub fn with_bootstrap_servers<T: AsRef<str>>(mut self, servers: T) -> Self {
        self.bootstrap_servers = servers.as_ref().to_string();
        self
    }

    pub fn with_message_timeout(mut self, timeout: u64) -> Self {
        self.message_timeout_ms = timeout;
        self
    }

    pub fn with_socket_timeout(mut self, timeout: u64) -> Self {
        self.socket_timeout_ms = timeout;
        self
    }

    pub fn with_serializer_config(mut self, serializer_config: SerializerConfig) -> Self {
        self.serializer_config = serializer_config;
        self
    }

    pub fn with_client_property<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.librdkafka_options.insert(key.into(), value.into());
        self
    }
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_owned(),
            topic: "topic-42".to_owned(),
            key_field: None,
            socket_timeout_ms: default_socket_timeout_ms(),
            message_timeout_ms: default_message_timeout_ms(),
            librdkafka_options: Default::default(),
            serializer_config: JsonSerializerConfig::default().into(),
        }
    }
}

impl SinkConfig for KafkaSinkConfig {
    fn build(&self) -> crate::Result<Box<dyn Sink>> {
        let sink = Box::new(KafkaSink::new(self.clone())?);
        Ok(sink)
    }

    fn health_check(&self) -> HealthCheck {
        health_check(self.clone()).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_to_rdkafka() {
        let cfg = KafkaSinkConfig::default();
        let result = cfg.to_rdkafka().expect("Unable to create config");
        assert_eq!(result.get("bootstrap.servers").unwrap(), "localhost:9092");
        assert_eq!(
            result.get("socket.timeout.ms").unwrap(),
            default_socket_timeout_ms().to_string()
        );
        assert_eq!(
            result.get("message.timeout.ms").unwrap(),
            default_message_timeout_ms().to_string()
        );
    }

    #[test]
    fn test_with_builder() {
        let expected = "127.0.0.1:9092";
        let cfg = KafkaSinkConfig::default()
            .with_bootstrap_servers(expected)
            .with_client_property("socket.timeout.ms".to_string(), "42");

        let result = cfg.to_rdkafka().expect("Unable to create config");
        assert_eq!(result.get("bootstrap.servers").unwrap(), expected);
        assert_eq!(result.get("socket.timeout.ms").unwrap(), "42");
    }

    #[test]
    fn test_sink_build() {
        let cfg = KafkaSinkConfig::default();
        let sink = cfg.build();
        assert!(sink.is_ok());
    }
}
