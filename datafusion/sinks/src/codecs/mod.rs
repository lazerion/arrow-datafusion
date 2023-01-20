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

use bytes::BytesMut;
use tokio_util::codec::Encoder;

use crate::{Error, Event};

use self::json::JsonSerializerConfig;

pub mod json;

#[derive(Clone, Debug)]
pub enum SerializerConfig {
    Json(
        /// Encoding options specific to the json serializer.
        JsonSerializerConfig,
    ),
}

impl SerializerConfig {
    /// Build the `Serializer` from this configuration.
    pub fn build(&self) -> crate::Result<Serializer> {
        match self {
            SerializerConfig::Json(config) => Ok(Serializer::Json(config.build())),
        }
    }
}

/// Serialize structured events as bytes.
#[derive(Debug, Clone)]
pub enum Serializer {
    Json(json::JsonSerializer),
}

impl From<JsonSerializerConfig> for SerializerConfig {
    fn from(config: JsonSerializerConfig) -> Self {
        Self::Json(config)
    }
}

impl Encoder<Event> for Serializer {
    type Error = Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match self {
            Serializer::Json(serializer) => serializer.encode(event, buffer),
        }
    }
}
