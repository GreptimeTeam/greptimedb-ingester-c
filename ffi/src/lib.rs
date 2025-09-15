// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::set_panic_hook;
use crate::inserter::Inserter;
use crate::logger::init_logger;
use crate::row::RowBuilder;
use greptimedb_client::api::v1::InsertRequest;
use snafu::OptionExt;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

mod error;
mod ffi;
mod inserter;
mod logger;
mod row;
mod util;

pub struct Client {
    runtime: Runtime,
    tx: Option<mpsc::Sender<InsertRequest>>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for Client {
    fn drop(&mut self) {
        info!("Dropping client");
    }
}

impl Client {
    pub fn new(database_name: String, addr: String) -> error::Result<Self> {
        init_logger();
        set_panic_hook();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicU8 = AtomicU8::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
                format!("gt-client-{}", id)
            })
            .build()
            .unwrap();
        let (tx, rx) = mpsc::channel(1024);

        let handle = runtime.spawn(async move {
            let mut inserter = Inserter::new(database_name, addr, rx).unwrap();
            inserter.run().await;
        });

        Ok(Self {
            runtime,
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    pub fn write_row(&self, row: &mut RowBuilder) -> error::Result<()> {
        self.tx
            .as_ref()
            .context(error::ClientStoppedSnafu)?
            .blocking_send(row.into())
            .map_err(|_| error::SendRequestSnafu {}.build())
    }

    pub fn stop(&mut self) {
        self.tx.take();
        let handle = self.handle.take();
        self.runtime.block_on(async move {
            if let Some(handle) = handle {
                handle.await.unwrap();
            }
        });
    }
}
