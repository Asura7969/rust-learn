use arrow_schema::DataType;
use datafusion::{
    datasource::{listing::ListingTableUrl, DefaultTableSource},
    error::Result,
};
use datafusion_common::{config::ConfigOptions, TableReference};
use datafusion_sql::planner::ContextProvider;
use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef, datasource::TableProvider, execution::context::SessionState,
    physical_plan::ExecutionPlan,
};
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, TableSource, TableType, WindowUDF};

use crate::datafusion::{
    builder::PaimonTableBuilder,
    paimon::{exec::MergeExec, reader::read_parquet},
};

use super::{
    error::PaimonError,
    snapshot::{Snapshot, SnapshotManager},
    to_schema_ref, PaimonSchema,
};

#[allow(dead_code)]
pub struct PaimonProvider {
    pub table_path: ListingTableUrl,
    pub(crate) snapshot: Snapshot,
}

#[allow(dead_code)]
impl PaimonProvider {
    pub fn new(table_path: ListingTableUrl, snapshot: Snapshot) -> PaimonProvider {
        PaimonProvider {
            table_path,
            snapshot,
        }
    }

    pub fn get_paimon_schema(&self) -> Result<PaimonSchema, PaimonError> {
        let table_path = &self.table_path;
        self.snapshot.get_schema(table_path.prefix().as_ref())
    }
}

#[async_trait]
impl TableProvider for PaimonProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let table_path = &self.table_path;
        let mut schema = self
            .snapshot
            .get_schema(table_path.prefix().as_ref())
            .expect("read schema failed ...");
        to_schema_ref(&mut schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table_path = self.table_path.prefix().as_ref();
        let mut paimon_schema = self.snapshot.get_schema(table_path).unwrap();
        let entries = self.snapshot.base(table_path).unwrap();
        let parquet_exec = read_parquet(table_path, &entries, &mut paimon_schema).unwrap();

        Ok(Arc::new(MergeExec::new(
            paimon_schema,
            Arc::new(parquet_exec),
        )))
    }
}

// #[derive(Default)]
pub struct PaimonContextProvider {
    options: ConfigOptions,
    manager: SnapshotManager,
    url: ListingTableUrl,
}

#[allow(dead_code)]
impl PaimonContextProvider {
    pub fn latest_snapshot(&self) -> Result<Arc<PaimonProvider>, PaimonError> {
        let snapshot = self
            .manager
            .latest_snapshot()
            .expect("not find latest snapshot");
        Ok(Arc::new(PaimonProvider {
            table_path: self.url.clone(),
            snapshot,
        }))
    }

    pub fn snapshot(&self, id: i64) -> Result<Arc<PaimonProvider>, PaimonError> {
        let snapshot = self
            .manager
            .snapshot(id)
            .unwrap_or_else(|_| panic!("read snapshot failed, id: {}", id));
        Ok(Arc::new(PaimonProvider {
            table_path: self.url.clone(),
            snapshot,
        }))
    }
}

impl ContextProvider for PaimonContextProvider {
    /// Select the snapshot to read
    /// like mytable  -> read latest snapshot
    ///      mytable$snapshot=1  -> read snapshot id = 1
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let table_name = name.table();
        // TODO: read tag
        let snapshot = match table_name.find("$snapshot=") {
            Some(index) => {
                let (_real_name, snapshot_id) = table_name.split_at(index + 1);
                let snapshot_id = snapshot_id
                    .parse::<i64>()
                    .expect("Snapshot id requires number, like mytable$snapshot=1");
                self.manager
                    .snapshot(snapshot_id)
                    .unwrap_or_else(|_| panic!("read snapshot failed, id: {}", snapshot_id))
            }
            _ => self
                .manager
                .latest_snapshot()
                .expect("not find latest snapshot"),
        };

        Ok(Arc::new(DefaultTableSource::new(Arc::new(
            PaimonProvider {
                table_path: self.url.clone(),
                snapshot,
            },
        ))))
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

pub async fn open_table(table_uri: impl AsRef<str>) -> datafusion::error::Result<PaimonProvider> {
    let table = PaimonTableBuilder::from_uri(table_uri).load().await?;
    Ok(table)
}

/// Same as `open_table`, but also accepts storage options to aid in building the table for a deduced
/// `StorageService`.
pub async fn open_table_with_storage_options(
    table_uri: impl AsRef<str>,
    storage_options: HashMap<String, String>,
) -> datafusion::error::Result<PaimonProvider> {
    let table = PaimonTableBuilder::from_uri(table_uri)
        .with_storage_options(storage_options)
        .load()
        .await?;
    Ok(table)
}

#[allow(dead_code)]
pub async fn open_table_with_version(
    _table_uri: impl AsRef<str>,
    _tag: impl AsRef<str>,
) -> datafusion::error::Result<PaimonProvider> {
    todo!()
}

#[allow(unused_imports)]
#[cfg(test)]
mod tests {
    use arrow::util::pretty::print_batches as arrow_print_batches;
    use datafusion::{
        datasource::provider_as_source,
        prelude::{SessionConfig, SessionContext},
    };
    use datafusion_expr::LogicalPlan;
    use datafusion_sql::{
        planner::{ParserOptions, SqlToRel},
        sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser},
    };

    use super::*;
    use std::{collections::hash_map::Entry, path::PathBuf};

    use crate::datafusion::{
        dialect::PaimonDialect,
        paimon::{
            error::PaimonError, test_paimonm_table_path, PartitionKeys, PrimaryKeys, WriteMode,
        },
    };

    #[allow(dead_code)]
    fn get_ctx(path: &str) -> Result<(SessionContext, Arc<PaimonProvider>), PaimonError> {
        let manager = SnapshotManager::new(path);

        let provider = PaimonContextProvider {
            options: ConfigOptions::default(),
            manager,
            url: ListingTableUrl::parse(path).unwrap(),
        };

        let provider = provider.latest_snapshot()?;

        let paimon_schema = provider.get_paimon_schema()?;

        let options = paimon_schema.options.clone();
        let pk = paimon_schema.primary_keys.clone();
        let partition_keys = paimon_schema.partition_keys;

        let write_mode = if pk.is_empty() {
            WriteMode::Appendonly
        } else {
            WriteMode::Changelog
        };

        // 设置上下文参数：主键、分区键、任务参数
        let primary_keys_ext = Arc::new(PrimaryKeys(pk));
        let partition_keys_ext = Arc::new(PartitionKeys(partition_keys));
        let write_mode_ext = Arc::new(write_mode);
        let session_config = SessionConfig::from_string_hash_map(options)?
            .with_extension(Arc::clone(&primary_keys_ext))
            .with_extension(Arc::clone(&partition_keys_ext))
            .with_extension(Arc::clone(&write_mode_ext));

        Ok((SessionContext::with_config(session_config), provider))
    }

    // #[tokio::test]
    // async fn table_test() -> Result<(), PaimonError> {
    //     let binding = test_paimonm_table_path("ods_mysql_paimon_points_5");
    //     let path = binding.to_str().unwrap();

    //     let sql = "select * from ods_mysql_paimon_points_5$snapshot=1";
    //     let (ctx, provider) = get_ctx(path)?;
    //     ctx.register_table("ods_mysql_paimon_points_5", provider)?;

    //     let df = ctx.sql(sql).await?;
    //     let batch = df.collect().await?;
    //     arrow_print_batches(&batch).unwrap();
    //     Ok(())
    // }

    // #[tokio::test]
    // async fn table_ctx_provider_test() -> Result<(), PaimonError> {
    //     let binding = test_paimonm_table_path("ods_mysql_paimon_points_5");
    //     let path = binding.to_str().unwrap();

    //     let sql = "select * from ods_mysql_paimon_points_5 OPTIONS('scan.snapshot-id' = '1')";
    //     let (ctx, _) = get_ctx(path)?;

    //     let dialect = PaimonDialect {};

    //     let statements = Parser::parse_sql(&dialect, sql)?;

    //     println!("{:?}", statements);

    //     let plan = statement_to_plan(
    //         &ctx.state(),
    //         statements[0].clone(),
    //         "ods_mysql_paimon_points_5",
    //     )
    //     .await?;

    //     let df = ctx.execute_logical_plan(plan).await?;

    //     let batch = df.collect().await?;
    //     arrow_print_batches(&batch).unwrap();
    //     Ok(())
    // }

    #[allow(dead_code)]
    pub async fn statement_to_plan(
        state: &SessionState,
        statement: Statement,
        path: &str,
    ) -> Result<LogicalPlan> {
        // let references = state.resolve_table_references(&statement)?;
        let path = test_paimonm_table_path(path);
        let manager = SnapshotManager::new(path.to_str().unwrap());
        let provider = PaimonContextProvider {
            options: ConfigOptions::default(),
            manager,
            url: ListingTableUrl::parse(path.to_str().unwrap()).unwrap(),
        };

        let enable_ident_normalization =
            state.config_options().sql_parser.enable_ident_normalization;
        let parse_float_as_decimal = state.config_options().sql_parser.parse_float_as_decimal;
        // for reference in references {
        //     let table = reference.table();
        //     let resolved = state.resolve_table_ref(&reference);
        //     if let Entry::Vacant(v) = provider.tables.entry(resolved.to_string()) {
        //         if let Ok(schema) = state.schema_for_ref(resolved) {
        //             if let Some(table) = schema.table(table).await {
        //                 v.insert(provider_as_source(table));
        //             }
        //         }
        //     }
        // }

        let query = SqlToRel::new_with_options(
            &provider,
            ParserOptions {
                parse_float_as_decimal,
                enable_ident_normalization,
            },
        );
        query.sql_statement_to_plan(statement)
    }
}
