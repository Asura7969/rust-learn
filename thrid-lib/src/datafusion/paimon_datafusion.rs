use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    datasource::{listing::ListingTableUrl, provider::TableProviderFactory, TableProvider},
    execution::context::SessionState,
};
use datafusion_cli::object_storage::{get_oss_object_store_builder, get_s3_object_store_builder};
use datafusion_common::DataFusionError;
use datafusion_expr::CreateExternalTable;
use object_store::{local::LocalFileSystem, ObjectStore};
use url::Url;

use super::paimon::table::{open_table, open_table_with_storage_options};

pub struct PaimonTableFactory {}

#[async_trait]
impl TableProviderFactory for PaimonTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        create_external_table(state, cmd).await?;

        let location = cmd.to_owned().location;

        let provider = if cmd.options.is_empty() {
            open_table(location).await?
        } else {
            open_table_with_storage_options(location, cmd.to_owned().options).await?
        };
        Ok(Arc::new(provider))
    }
}

async fn create_external_table(
    state: &SessionState,
    cmd: &CreateExternalTable,
) -> datafusion::error::Result<()> {
    let table_path = ListingTableUrl::parse(&cmd.location)?;
    let scheme = table_path.scheme();
    let url: &Url = table_path.as_ref();

    // registering the cloud object store dynamically using cmd.options
    let store = match scheme {
        "s3" => {
            let builder = get_s3_object_store_builder(url, cmd).await?;
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }
        "oss" => {
            let builder = get_oss_object_store_builder(url, cmd)?;
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }
        "file" => Arc::new(LocalFileSystem::new_with_prefix(Path::new(
            &table_path.as_str(),
        ))?),
        _ => {
            // for other types, try to get from the object_store_registry
            state
                .runtime_env()
                .object_store_registry
                .get_store(url)
                .map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Unsupported object store scheme: {}",
                        scheme
                    ))
                })?
        }
    };

    state.runtime_env().register_object_store(url, store);

    Ok(())
}
