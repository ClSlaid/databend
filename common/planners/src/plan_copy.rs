// Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;

use common_datavalues2::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::MetaId;

use crate::UserStagePlan;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub struct CopyPlan {
    pub db_name: String,
    pub tbl_name: String,
    pub tbl_id: MetaId,
    pub schema: DataSchemaRef,
    pub stage: UserStagePlan,
}

impl CopyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
