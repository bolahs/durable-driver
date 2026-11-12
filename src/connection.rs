use async_trait::async_trait;
use send_wrapper::SendWrapper;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use toasty::schema::db::{self, AppliedMigration};
use toasty_core::stmt::{ValueRecord, ValueStream};
use toasty_core::{Error, Result};
use toasty_core::{
    Schema,
    driver::{
        Capability, Connection, ExecResponse,
        operation::{Operation, Transaction},
    },
};
use toasty_core::{
    driver::{
        IsolationLevel, QueryLogConfig,
        log::QueryLog,
        operation::{RawSqlRet, TypedValue},
    },
    schema::db::Table,
};
use toasty_sql::{Statement, serializer::Serializer, stmt::ExprSet};
use worker::{SqlStorage, SqlStorageValue};

use crate::helpers::{
    SqlReturn, cf_to_toasty_value, cf_to_toasty_value_with_expected_type, toasty_to_cf_value,
};

#[derive(Debug)]
pub struct DurableSqliteConnection {
    pub(crate) sql: Arc<SendWrapper<SqlStorage>>,
    pub(crate) query_log: QueryLogConfig,
}

impl DurableSqliteConnection {
    fn create_table(&mut self, schema: &db::Schema, table: &Table) -> Result<()> {
        let serializer = Serializer::sqlite(schema);

        let stmt = serializer.serialize(&Statement::create_table(table, &Capability::SQLITE));

        self.sql
            .exec(&stmt, None)
            .map_err(toasty_core::Error::driver_operation_failed)?;

        // Create any indices
        for index in &table.indices {
            // The PK has already been created by the table statement
            if index.primary_key {
                continue;
            }

            let stmt = serializer.serialize(&Statement::create_index(index));

            self.sql
                .exec(&stmt, None)
                .map_err(toasty_core::Error::driver_operation_failed)?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct TMigration {
    id: u64,
}

#[async_trait]
impl Connection for DurableSqliteConnection {
    async fn exec(&mut self, schema: &Arc<Schema>, op: Operation) -> Result<ExecResponse> {
        let (sql, typed_params, ret_tys) = match op {
            Operation::Insert(op) => (Statement::from(op.stmt), op.params, op.ret),
            Operation::QuerySql(op) => (Statement::from(op.stmt), op.params, op.ret),
            Operation::RawSql(op) => {
                let ret = match op.ret {
                    RawSqlRet::None => SqlReturn::Count,
                    RawSqlRet::Infer => SqlReturn::Infer,
                    RawSqlRet::Types(types) => SqlReturn::Types(types),
                };
                return self.exec_sql(&op.sql, op.params, ret);
            }
            Operation::Transaction(mut op) => {
                if let Transaction::Start { isolation, .. } = &mut op {
                    if !matches!(isolation, Some(IsolationLevel::Serializable) | None) {
                        return Err(toasty_core::Error::unsupported_feature(
                            "SQLite only supports Serializable isolation",
                        ));
                    }
                    *isolation = None;
                }
                let sql = Serializer::sqlite(&schema.db).serialize_transaction(&op);
                self.sql
                    .exec(&sql, None)
                    .map_err(toasty_core::Error::driver_operation_failed)?;
                return Ok(ExecResponse::count(0));
            }
            _ => todo!("op={:#?}", op),
        };

        let ret = match &sql {
            Statement::Query(stmt) => match &stmt.body {
                ExprSet::Select(_) => SqlReturn::Types(ret_tys.unwrap()),
                _ => todo!(),
            },
            Statement::Insert(stmt) => stmt
                .returning
                .as_ref()
                .map(|_| SqlReturn::Types(ret_tys.unwrap()))
                .unwrap_or(SqlReturn::Count),
            Statement::Delete(stmt) => stmt
                .returning
                .as_ref()
                .map(|_| SqlReturn::Types(ret_tys.unwrap()))
                .unwrap_or(SqlReturn::Count),
            Statement::Update(stmt) => {
                assert!(stmt.condition.is_none(), "stmt={stmt:#?}");
                stmt.returning
                    .as_ref()
                    .map(|_| SqlReturn::Types(ret_tys.unwrap()))
                    .unwrap_or(SqlReturn::Count)
            }
            _ => SqlReturn::Count,
        };

        let sql_str = Serializer::sqlite(&schema.db).serialize(&sql);
        self.exec_sql(&sql_str, typed_params, ret)
    }

    async fn push_schema(&mut self, schema: &Schema) -> Result<()> {
        for table in &schema.db.tables {
            self.create_table(&schema.db, table)?;
        }

        Ok(())
    }

    async fn applied_migrations(
        &mut self,
    ) -> Result<Vec<toasty_core::schema::db::AppliedMigration>> {
        // Ensure the migrations table exists
        self.sql
            .exec(
                "CREATE TABLE IF NOT EXISTS __toasty_migrations (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL
            )",
                None,
            )
            .map_err(toasty_core::Error::driver_operation_failed)?;

        // Query all applied migrations
        let ids: Vec<TMigration> = self
            .sql
            .exec(
                "SELECT id FROM __toasty_migrations ORDER BY applied_at",
                None,
            )
            .map_err(toasty_core::Error::driver_operation_failed)?
            .to_array()
            .map_err(toasty_core::Error::driver_operation_failed)?;

        Ok(ids.iter().map(|m| AppliedMigration::new(m.id)).collect())
    }

    async fn apply_migration(
        &mut self,
        id: u64,
        name: &str,
        migration: &toasty_core::schema::db::Migration,
    ) -> Result<()> {
        // tracing::info!(id = id, name = %name, "applying migration");
        // Ensure the migrations table exists
        self.sql
            .exec(
                "CREATE TABLE IF NOT EXISTS __toasty_migrations (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL
            )",
                None,
            )
            .map_err(toasty_core::Error::driver_operation_failed)?;

        // Start transaction
        self.sql
            .exec("BEGIN", None)
            .map_err(toasty_core::Error::driver_operation_failed)?;

        // Execute each migration statement
        for statement in migration.statements() {
            if let Err(e) = self
                .sql
                .exec(statement, None)
                .map_err(toasty_core::Error::driver_operation_failed)
            {
                self.sql
                    .exec("ROLLBACK", None)
                    .map_err(toasty_core::Error::driver_operation_failed)?;
                return Err(e);
            }
        }

        // Record the migration
        if let Err(e) = self.sql.exec(
            "INSERT INTO __toasty_migrations (id, name, applied_at) VALUES (?1, ?2, datetime('now'))",
            vec![SqlStorageValue::Integer(id as i64), SqlStorageValue::String(String::from(name))],
        ).map_err(toasty_core::Error::driver_operation_failed) {
            self.sql.exec("ROLLBACK", None).map_err(toasty_core::Error::driver_operation_failed)?;
            return Err(e);
        }

        // Commit transaction
        self.sql
            .exec("COMMIT", None)
            .map_err(toasty_core::Error::driver_operation_failed)?;
        Ok(())
    }
}

impl DurableSqliteConnection {
    fn exec_sql(
        &mut self,
        sql_str: &str,
        typed_params: Vec<TypedValue>,
        ret: SqlReturn,
    ) -> Result<ExecResponse> {
        let mut log = QueryLog::sql(
            &self.query_log,
            "sqlite",
            sql_str,
            typed_params.iter().map(|tv| &tv.value),
        );
        let result = self.exec_sql_inner(sql_str, typed_params, ret, &mut log);
        log.finish(&result);
        result
    }

    fn exec_sql_inner(
        &mut self,
        sql_str: &str,
        typed_params: Vec<TypedValue>,
        ret: SqlReturn,
        _log: &mut QueryLog<'_>,
    ) -> Result<ExecResponse> {
        let bindings = typed_params
            .into_iter()
            .map(|tv| toasty_to_cf_value(tv.value).unwrap())
            .collect::<Vec<_>>();

        let cursor = self
            .sql
            .exec(sql_str, bindings)
            .map_err(toasty_core::Error::driver_operation_failed)?;

        if matches!(ret, SqlReturn::Count) {
            let count = cursor.count();
            return Ok(ExecResponse::count(count as _));
        }

        let column_count = cursor.column_names().len();

        let mut values = vec![];

        let raw_rows = cursor.raw();
        for row in raw_rows {
            if let Ok(row) = row {
                let items = match &ret {
                    SqlReturn::Count => unreachable!(),
                    SqlReturn::Infer => (0..column_count)
                        //.map(|index| Value::from_sql_infer(row, index).into_inner())
                        .map(|index| {
                            cf_to_toasty_value(
                                row.get(index)
                                    .unwrap_or(&SqlStorageValue::Null)
                                    .clone(),
                            )
                        })
                        .collect(),
                    SqlReturn::Types(ret_tys) => ret_tys
                        .iter()
                        .enumerate()
                        .map(|(index, ret_ty)| {
                            cf_to_toasty_value_with_expected_type(
                                row.get(index)
                                    .unwrap_or(&SqlStorageValue::Null)
                                    .clone(),
                                ret_ty,
                            )
                        })
                        //.map(|(index, ret_ty)| Value::from_sql(row, index, ret_ty).into_inner())
                        .collect(),
                };

                values.push(ValueRecord::from_vec(items).into());
            }
        }

        Ok(ExecResponse::value_stream(ValueStream::from_vec(values)))
    }

    fn exec_tx(&mut self, tx: Transaction) -> Result<ExecResponse> {
        let cmd = match tx {
            Transaction::Start { .. } => "BEGIN TRANSACTION;",
            Transaction::Commit => "COMMIT;",
            Transaction::Rollback => "ROLLBACK;",
            Transaction::Savepoint(_) => todo!(),
            Transaction::ReleaseSavepoint(_) => todo!(),
            Transaction::RollbackToSavepoint(_) => todo!(),
        };

        self.sql
            .exec(cmd, None)
            .map_err(Error::driver_operation_failed)?;

        Ok(ExecResponse::empty_value_stream())
    }
}
