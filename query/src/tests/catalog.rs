// Copyright 2020 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::Result;

use crate::catalogs::impls::DatabaseCatalog;
use crate::catalogs::impls::RemoteMetaStoreClient;
use crate::configs::Config;
use crate::datasources::local::LocalFactory;
use crate::datasources::remote::RemoteFactory;
use crate::datasources::system::SystemFactory;

pub fn try_create_catalog() -> Result<DatabaseCatalog> {
    let conf = Config::default();
    let remote_factory = RemoteFactory::new(&conf);
    let store_client_provider = remote_factory.store_client_provider();
    let cli = Arc::new(RemoteMetaStoreClient::create(Arc::new(
        store_client_provider,
    )));
    let catalog = DatabaseCatalog::try_create_with_config(conf.clone(), cli)?;
    let system = SystemFactory::create().load_databases()?;
    catalog.register_databases(system)?;
    if conf.store.disable_local_database_engine == "0" {
        let local = LocalFactory::create().load_databases()?;
        catalog.register_databases(local)?;
    }
    Ok(catalog)
}
