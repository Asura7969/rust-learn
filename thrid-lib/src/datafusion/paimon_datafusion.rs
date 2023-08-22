use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    datasource::{provider::TableProviderFactory, TableProvider},
    execution::context::SessionState,
};
use datafusion_expr::CreateExternalTable;

use super::paimon::table::{open_table, open_table_with_storage_options};

pub struct PaimonTableFactory {}

#[async_trait]
impl TableProviderFactory for PaimonTableFactory {
    async fn create(
        &self,
        _ctx: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let provider = if cmd.options.is_empty() {
            open_table(cmd.to_owned().location).await?
        } else {
            open_table_with_storage_options(cmd.to_owned().location, cmd.to_owned().options).await?
        };
        Ok(Arc::new(provider))
    }
}
