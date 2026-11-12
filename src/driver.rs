use async_trait::async_trait;
use send_wrapper::SendWrapper;
use serde::Serialize;
use std::borrow::Cow;
use std::sync::Arc;
use toasty::schema::{
        db::Migration,
        diff,
    };
use toasty_core::Result;
use toasty_core::driver::{
        Capability, ConnectContext, Connection, Driver,
    };
use toasty_sql::{MigrationStatement, serializer::Serializer};
use worker::SqlStorage;

use crate::DurableSqliteConnection;

/// Wraps Cloudflare Durable Object `SqlStorage` for Toasty.
/// Uses `SendWrapper` because Workers run on a single-threaded WASM runtime.
#[derive(Clone, Debug)]
pub struct DurableSqliteDriver {
    sql: Arc<SendWrapper<SqlStorage>>,
}

impl DurableSqliteDriver {
    pub fn new(sql: SqlStorage) -> Self {
        Self {
            sql: Arc::new(SendWrapper::new(sql)),
        }
    }
}

#[async_trait]
impl Driver for DurableSqliteDriver {
    fn url(&self) -> Cow<'_, str> {
        Cow::Borrowed("durable-object://sqlite")
    }

    fn capability(&self) -> &'static Capability {
        // Use Toasty's built-in SQLite capability flags
        &Capability::SQLITE
    }

    fn max_connections(&self) -> Option<usize> {
        // Durable Object SQLite is single-threaded and embedded in-process
        Some(1)
    }

    async fn connect(&self, ctx: &ConnectContext) -> Result<Box<dyn Connection>> {
        Ok(Box::new(DurableSqliteConnection {
            sql: self.sql.clone(),
            query_log: ctx.query_log,
        }))
    }

    fn generate_migration(&self, schema_diff: &diff::Schema<'_>) -> Migration {
        let statements = MigrationStatement::from_diff(schema_diff, &Capability::SQLITE);

        let sql_strings: Vec<String> = statements
            .iter()
            .map(|stmt| Serializer::sqlite(stmt.schema()).serialize(stmt.statement()))
            .collect();

        Migration::new_sql_with_breakpoints(&sql_strings)
    }

    async fn reset_db(&self) -> toasty_core::Result<()> {
        Ok(())
    }
}
