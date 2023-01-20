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

use std::{collections::BTreeMap, num::NonZeroU64};

use chrono::{DateTime, Utc};
use serde::Serialize;

pub mod api;
pub mod codecs;
pub mod kafka;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(PartialEq, Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    Aggregation(AggregationData),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AggregationData {
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<DateTime<Utc>>,

    /// The interval, in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    interval_ms: Option<NonZeroU64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<BTreeMap<String, String>>,
}

impl Default for AggregationData {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregationData {
    pub fn new() -> Self {
        Self {
            timestamp: None,
            interval_ms: None,
            tags: None,
        }
    }
    pub fn tags(&self) -> Option<&BTreeMap<String, String>> {
        self.tags.as_ref()
    }

    pub fn tags_mut(&mut self) -> Option<&mut BTreeMap<String, String>> {
        self.tags.as_mut()
    }

    pub fn with_tags(mut self, tags: Option<BTreeMap<String, String>>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_interval_ms(mut self, interval_ms: Option<NonZeroU64>) -> Self {
        self.interval_ms = interval_ms;
        self
    }

    pub fn with_timestamp(mut self, timestamp: Option<DateTime<Utc>>) -> Self {
        self.timestamp = timestamp;
        self
    }
}
