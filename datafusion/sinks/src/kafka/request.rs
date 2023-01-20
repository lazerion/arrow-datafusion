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

use crate::codecs::Serializer;
use crate::Event;
use bytes::{Bytes, BytesMut};
use tokio_util::codec::Encoder;

pub struct KafkaRequest {
    pub body: Bytes,
    pub key: Option<Bytes>,
}

pub struct KafkaRequestBuilder {
    pub key_field: Option<String>,
    pub serializer: Serializer,
}

impl KafkaRequestBuilder {
    pub fn build_request(&mut self, event: Event) -> Option<KafkaRequest> {
        let key = self.get_key(&event);
        let mut body = BytesMut::new();
        self.serializer.encode(event, &mut body).ok()?;
        let body = body.freeze();
        Some(KafkaRequest { body, key })
    }

    fn get_key(&self, event: &Event) -> Option<Bytes> {
        self.key_field.as_ref().and_then(|key_field| match event {
            Event::Aggregation(agg) => agg
                .tags()
                .and_then(|f| f.get(key_field))
                .map(|f| f.to_owned().into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::codecs::json::JsonSerializer;
    use crate::AggregationData;
    use std::{collections::BTreeMap, num::NonZeroU64};

    use super::*;

    #[test]
    fn test_build() {
        let mut rb = KafkaRequestBuilder {
            key_field: None,
            serializer: Serializer::Json(JsonSerializer::new()),
        };
        let data = AggregationData {
            timestamp: None,
            interval_ms: NonZeroU64::new(42),
            tags: Some(BTreeMap::new()),
        };
        let event = Event::Aggregation(data);
        let actual = rb.build_request(event);
        assert!(actual.is_some());
    }

    #[test]
    fn test_get_key() {
        let rb = KafkaRequestBuilder {
            key_field: Some("key".to_owned()),
            serializer: Serializer::Json(JsonSerializer::new()),
        };
        let mut data = AggregationData {
            timestamp: None,
            interval_ms: NonZeroU64::new(42),
            tags: Some(BTreeMap::new()),
        };
        data.tags_mut()
            .and_then(|t| t.insert("key".to_owned(), "42".to_owned()));

        let event = Event::Aggregation(data);
        let actual = rb.get_key(&event).unwrap();
        assert_eq!(&actual[..], b"42");
    }

    #[test]
    fn test_none_key() {
        let rb = KafkaRequestBuilder {
            key_field: Some("key".to_owned()),
            serializer: Serializer::Json(JsonSerializer::new()),
        };
        let data = AggregationData {
            timestamp: None,
            interval_ms: NonZeroU64::new(42),
            tags: Some(BTreeMap::new()),
        };

        let event = Event::Aggregation(data);
        let actual = rb.get_key(&event);
        assert!(actual.is_none());
    }
}
