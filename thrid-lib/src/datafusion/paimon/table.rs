use datafusion::{datasource::listing::ListingTableUrl, error::Result};
use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef, datasource::TableProvider, execution::context::SessionState,
    physical_plan::ExecutionPlan,
};
use datafusion_expr::{Expr, TableType};

use crate::datafusion::paimon::{exec::MergeExec, reader::read_parquet};

use super::{get_latest_metedata_file, snapshot::SnapshotManager, to_schema_ref};

#[allow(dead_code)]
pub struct PaimonDataSource {
    pub table_path: ListingTableUrl,
    pub(crate) snapshot_manager: SnapshotManager,
}

#[allow(dead_code)]
impl PaimonDataSource {
    pub fn new(table_path: ListingTableUrl) -> PaimonDataSource {
        let path = &table_path.as_str().to_string();
        PaimonDataSource {
            table_path,
            snapshot_manager: SnapshotManager::new(path),
        }
    }
}

#[async_trait]
impl TableProvider for PaimonDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let table_path = &self.table_path;
        let snapshot =
            get_latest_metedata_file(table_path.as_str()).expect("read snapshot failed ...");
        let mut schema = snapshot
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
        // let schema = self.schema();
        let snapshot = self.snapshot_manager.latest_snapshot().unwrap();
        let mut schema = snapshot.get_schema(self.table_path.as_str()).unwrap();
        let entries = snapshot.base(self.table_path.as_str()).unwrap();
        let parquet_exec = read_parquet(self.table_path.as_str(), &entries, &mut schema).unwrap();
        Ok(Arc::new(MergeExec::new(Arc::new(parquet_exec))))
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
