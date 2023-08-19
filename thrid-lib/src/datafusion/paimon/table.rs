use arrow_schema::DataType;
use datafusion::{datasource::listing::ListingTableUrl, error::Result};
use datafusion_common::{config::ConfigOptions, TableReference};
use datafusion_sql::planner::ContextProvider;
use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef, datasource::TableProvider, execution::context::SessionState,
    physical_plan::ExecutionPlan,
};
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, TableSource, TableType, WindowUDF};

use crate::datafusion::paimon::{exec::MergeExec, reader::read_parquet};

use super::{
    snapshot::{Snapshot, SnapshotManager},
    to_schema_ref,
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
            .get_schema(table_path.as_str())
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
        let mut schema = self.snapshot.get_schema(self.table_path.as_str()).unwrap();
        let entries = self.snapshot.base(self.table_path.as_str()).unwrap();
        let parquet_exec = read_parquet(self.table_path.as_str(), &entries, &mut schema).unwrap();
        Ok(Arc::new(MergeExec::new(Arc::new(parquet_exec))))
    }
}

pub struct PaimonDataSource {
    pub table_provider: Arc<dyn TableProvider>,
}

impl PaimonDataSource {
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Self {
        Self { table_provider }
    }
}

impl TableSource for PaimonDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_provider.schema()
    }

    fn get_logical_plan(&self) -> Option<&datafusion_expr::LogicalPlan> {
        self.table_provider.get_logical_plan()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<datafusion_expr::TableProviderFilterPushDown> {
        Ok(datafusion_expr::TableProviderFilterPushDown::Unsupported)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<datafusion_expr::TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|f| {
                let _this = &self;
                let _filter: &Expr = f;
                Ok(datafusion_expr::TableProviderFilterPushDown::Unsupported)
            })
            .collect()
    }
}

// #[derive(Default)]
pub struct PaimonContextProvider {
    options: ConfigOptions,
    manager: SnapshotManager,
    url: ListingTableUrl,
}

impl ContextProvider for PaimonContextProvider {
    /// Select the snapshot to read
    /// like mytable  -> read latest snapshot
    ///      mytable$1  -> read snapshot id = 1
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let table_name = name.table();
        let snapshot = match table_name.find('$') {
            Some(index) => {
                let (_real_name, snapshot_id) = table_name.split_at(index + 1);
                let snapshot_id = snapshot_id
                    .parse::<i64>()
                    .expect("Snapshot id requires number, like mytable$1");
                self.manager
                    .snapshot(snapshot_id)
                    .unwrap_or_else(|_| panic!("read snapshot failed, id: {}", snapshot_id))
            }
            _ => self
                .manager
                .latest_snapshot()
                .expect("not find latest snapshot"),
        };

        Ok(Arc::new(PaimonDataSource::new(Arc::new(PaimonProvider {
            table_path: self.url.clone(),
            snapshot,
        }))))
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

#[allow(unused_imports)]
#[cfg(test)]
mod tests {
    use crate::datafusion::paimon::error::PaimonError;

    use super::*;
    #[tokio::test]
    async fn table_test() -> Result<(), PaimonError> {
        let url = ListingTableUrl::parse("file:///foo/file").unwrap();
        println!("{}", url.as_str());
        Ok(())
    }
}
