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
use crate::logger::init_logger;
use crate::row::RowBuilder;
use greptimedb_ingester::api::v1::auth_header::AuthScheme;
use greptimedb_ingester::api::v1::{Basic, RowInsertRequest, RowInsertRequests};
use greptimedb_ingester::database::Database;
use snafu::ResultExt;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio::runtime::Runtime;

mod error;
mod ffi;
mod logger;
mod row;
mod util;

pub struct Client {
    runtime: Runtime,
    client: Database,
}

impl Drop for Client {
    fn drop(&mut self) {
        info!("Dropping client");
    }
}

impl Client {
    pub fn new(
        database_name: String,
        addr: String,
        auth: Option<(String, String)>,
    ) -> error::Result<Self> {
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

        let client = greptimedb_ingester::client::Client::with_urls(vec![&addr]);
        let mut client = Database::new_with_dbname(database_name, client);
        if let Some((username, password)) = auth {
            client.set_auth(AuthScheme::Basic(Basic { username, password }));
        }

        Ok(Self { runtime, client })
    }

    pub fn write_row(&self, row: &mut RowBuilder) -> error::Result<()> {
        let insert_req: RowInsertRequest = row.into();
        let insert_reqs = RowInsertRequests {
            inserts: vec![insert_req],
        };
        self.runtime
            .block_on(self.client.insert(insert_reqs))
            .map_err(Box::new)
            .context(error::InsertReqSnafu)?;
        Ok(())
    }
}
