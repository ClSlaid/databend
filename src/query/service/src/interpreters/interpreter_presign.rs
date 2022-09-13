// Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use serde_json::Value;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::PresignAction;
use crate::sql::plans::PresignPlan;
use crate::storages::stage::StageSourceHelper;

pub struct PresignInterpreter {
    ctx: Arc<dyn TableContext>,
    plan: PresignPlan,
}

impl PresignInterpreter {
    /// Create a PresignInterpreter with context and [`PresignPlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: PresignPlan) -> Result<Self> {
        Ok(PresignInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for PresignInterpreter {
    fn name(&self) -> &str {
        "PresignInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[tracing::instrument(level = "debug", name = "presign_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let op = StageSourceHelper::get_op(&self.ctx, &self.plan.stage).await?;
        if !op.metadata().can_presign() {
            return Err(ErrorCode::StorageUnsupported(
                "storage doesn't support presign operation",
            ));
        }

        let o = op.object(&self.plan.path);
        let presigned_req = match self.plan.action {
            PresignAction::Download => o.presign_read(self.plan.expire)?,
            PresignAction::Upload => o.presign_write(self.plan.expire)?,
        };

        let block = DataBlock::create(self.plan.schema(), vec![
            Series::from_data(vec![presigned_req.method().as_str()]),
            Series::from_data(vec![VariantValue(Value::Object(
                presigned_req
                    .header()
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k.to_string(),
                            Value::String(
                                v.to_str()
                                    .expect("header value generated by opendal must be valid")
                                    .to_string(),
                            ),
                        )
                    })
                    .collect(),
            ))]),
            Series::from_data(vec![presigned_req.uri().to_string()]),
        ]);

        PipelineBuildResult::from_blocks(vec![block])
    }
}
