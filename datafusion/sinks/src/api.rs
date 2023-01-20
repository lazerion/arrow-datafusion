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

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::{Error, Event};

pub enum Sinks {
    Kafka,
}

pub type HealthCheck = BoxFuture<'static, crate::Result<()>>;

#[async_trait]
pub trait Sink {
    async fn run(self: Box<Self>, input: Event) -> Result<(), Error>;
}

pub trait SinkConfig: core::fmt::Debug + Send + Sync {
    fn build(&self) -> crate::Result<Box<dyn Sink>>;
    fn health_check(&self) -> HealthCheck;
}
