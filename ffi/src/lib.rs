use crate::error::set_panic_hook;
use crate::inserter::Inserter;
use crate::logger::init_logger;
use crate::row::RowBuilder;
use greptimedb_client::api::v1::InsertRequest;
use std::sync::RwLock;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

mod error;
mod ffi;
mod inserter;
mod logger;
mod row;

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
            .build()
            .unwrap();
        let (tx, rx) = mpsc::channel(1024);

        // todo: maybe store task handle.
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

    pub fn write_row(&self, row: &mut RowBuilder) {
        self.tx
            .as_ref()
            .unwrap()
            .blocking_send(row.try_into().unwrap());
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
