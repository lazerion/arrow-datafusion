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

use crate::{Error, Event};
use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;

#[derive(Debug, Clone)]
pub struct JsonSerializer {}

#[derive(Debug, Clone, Default)]
pub struct JsonSerializerConfig {}

impl JsonSerializerConfig {
    /// Build the `JsonSerializer` from this configuration.
    pub const fn build(&self) -> JsonSerializer {
        JsonSerializer::new()
    }
}

/// Serializer that converts an `Event` to bytes using the JSON format.
impl JsonSerializer {
    /// Creates a new `JsonSerializer`.
    pub const fn new() -> Self {
        Self {}
    }
}

impl Encoder<Event> for JsonSerializer {
    type Error = Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let writer = buffer.writer();
        match event {
            Event::Aggregation(agg) => serde_json::to_writer(writer, &agg),
        }
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use chrono::prelude::*;
    use crate::AggregationData;
    use std::{collections::BTreeMap, num::NonZeroU64};

    use super::*;

    #[test]
    fn test_encode_data() {
        let dt= Utc.with_ymd_and_hms(2042, 7, 8, 9, 10, 11).unwrap();
        let mut data = AggregationData {
            timestamp: Some(dt),
            interval_ms: NonZeroU64::new(42),
            tags: Some(BTreeMap::new()),
        };
        data.tags_mut()
            .and_then(|t| t.insert("bucket".to_owned(), "42".to_owned()));

        let event = Event::Aggregation(data);
        let config = JsonSerializerConfig {};
        let mut serializer = config.build();
        let mut body = BytesMut::new();
        let result = serializer.encode(event, &mut body);
        assert!(result.is_ok());
        assert_eq!(
            body.freeze(),
            r#"{"timestamp":"2042-07-08T09:10:11Z","interval_ms":42,"tags":{"bucket":"42"}}"#
        );
    }
}
