// Copyright 2021 Datafuse Labs
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

use async_trait::async_trait;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use paimon::table::CommitMessage;

use crate::error::map_paimon_error;
use crate::write::meta::PaimonCommitMeta;

/// Coordinator sink that collects writer commit metas and submits one snapshot.
pub struct PaimonCommitSink {
    table: paimon::Table,
    messages: Vec<CommitMessage>,
}

impl PaimonCommitSink {
    pub fn new(table: paimon::Table) -> Self {
        Self {
            table,
            messages: Vec::new(),
        }
    }

    pub fn try_create(input: Arc<InputPort>, table: paimon::Table) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(
            input,
            Self::new(table),
        )))
    }
}

#[async_trait]
impl AsyncSink for PaimonCommitSink {
    const NAME: &'static str = "PaimonCommitSink";
    // Match Fuse multi_table_insert_commit: on pipeline abort after partial
    // consume, do not call on_finish() — otherwise we would still commit.
    const CALL_ON_FINISH_ON_ERROR: bool = false;

    async fn consume(&mut self, block: DataBlock) -> Result<bool> {
        if let Some(meta) = block.get_meta() {
            let meta = PaimonCommitMeta::downcast_ref_from(meta).ok_or_else(|| {
                ErrorCode::Internal("invalid Paimon commit meta".to_string())
            })?;
            self.messages.extend(meta.clone().into_messages()?);
        }
        Ok(false)
    }

    async fn on_finish(&mut self) -> Result<()> {
        if self.messages.is_empty() {
            return Ok(());
        }
        self.table
            .new_write_builder()
            .new_commit()
            .commit(std::mem::take(&mut self.messages))
            .await
            .map_err(map_paimon_error)
    }
}
