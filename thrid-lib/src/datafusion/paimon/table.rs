use datafusion::arrow::datatypes::SchemaRef;

use super::{snapshot::SnapshotManager, to_schema_ref};

#[allow(dead_code)]
pub struct PaimonTable<'a> {
    pub table_path: &'a str,
    snapshot_manager: SnapshotManager,
}

#[allow(dead_code)]
impl<'a> PaimonTable<'a> {
    pub fn new(table_path: &'a str) -> PaimonTable<'a> {
        PaimonTable {
            table_path,
            snapshot_manager: SnapshotManager::new(table_path),
        }
    }

    pub fn schema(&self) -> Option<SchemaRef> {
        if let Some(snapshot) = self.snapshot_manager.latest_snapshot() {
            let mut schema = snapshot
                .get_schema(self.table_path)
                .expect("read schema failed ...");
            Some(to_schema_ref(&mut schema))
        } else {
            None
        }
    }
}
