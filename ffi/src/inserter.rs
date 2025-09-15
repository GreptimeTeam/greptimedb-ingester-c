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

use crate::error;
use greptimedb_client::{api::v1::InsertRequest, Database};

pub struct Inserter {
    client: Database,
    insert_request_receiver: tokio::sync::mpsc::Receiver<InsertRequest>,
}

impl Inserter {
    pub fn new(
        db_name: String,
        grpc_endpoint: String,
        insert_request_receiver: tokio::sync::mpsc::Receiver<InsertRequest>,
    ) -> error::Result<Self> {
        let grpc_client = greptimedb_client::Client::with_urls(vec![&grpc_endpoint]);
        let client = greptimedb_client::Database::new_with_dbname(db_name, grpc_client);
        Ok(Self {
            client,
            insert_request_receiver,
        })
    }

    pub async fn run(&mut self) {
        while let Some(request) = self.insert_request_receiver.recv().await {
            if let Err(e) = self.client.insert(vec![request]).await {
                error!("Failed to send requests to database, error: {:?}", e);
                break;
            }
        }
    }
}

// pub fn maybe_split_insert_request(
//     req: InsertRequest,
// ) -> Box<dyn Iterator<Item = InsertRequest> + Send> {
//     const BATCH_SIZE: u32 = 1024;
//     if req.row_count > BATCH_SIZE {
//         let chunks = chunks(req.row_count as usize, BATCH_SIZE as usize);
//         debug!("Splitting request into {}", chunks.len());
//
//         let iter = chunks.into_iter().map(move |(lower, upper)| {
//             let mut columns = Vec::with_capacity(req.columns.len());
//
//             for col in &req.columns {
//                 let values = col.values.as_ref().map(|v| take_values(v, lower, upper));
//
//                 columns.push(Column {
//                     column_name: col.column_name.clone(),
//                     semantic_type: col.semantic_type,
//                     values,
//                     null_mask: col.null_mask.clone(),
//                     datatype: col.datatype,
//                 });
//             }
//             InsertRequest {
//                 table_name: req.table_name.clone(),
//                 columns,
//                 row_count: (upper - lower) as u32,
//                 region_number: 0,
//             }
//         });
//         Box::new(iter)
//     } else {
//         Box::new(std::iter::once(req))
//     }
// }
