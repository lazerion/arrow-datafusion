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

use std::task::{Context, Poll};

use futures_core::future::BoxFuture;
use rdkafka::{
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use tower::Service;

use super::request::KafkaRequest;

#[derive(Debug)]
#[allow(dead_code)]
pub struct KafkaResponse {
    partition: i32,
    offset: i64,
}

#[derive(Clone)]
pub struct KafkaService {
    producer: FutureProducer,
    topic: String,
}

impl KafkaService {
    pub(crate) fn new(producer: FutureProducer, topic: String) -> KafkaService {
        KafkaService { producer, topic }
    }
}

impl Service<KafkaRequest> for KafkaService {
    type Response = KafkaResponse;
    type Error = KafkaError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: KafkaRequest) -> Self::Future {
        let this = self.clone();

        Box::pin(async move {
            let mut record = FutureRecord::to(&this.topic).payload(request.body.as_ref());
            if let Some(key) = &request.key {
                record = record.key(&key[..]);
            }

            // rdkafka will internally retry forever if the queue is full
            match this.producer.send(record, Timeout::Never).await {
                Ok((partition, offset)) => Ok(KafkaResponse { partition, offset }),
                Err((kafka_err, _original_record)) => Err(kafka_err),
            }
        })
    }
}
