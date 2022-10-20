//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_arrow::avro::file::CompressedBlock;
use common_arrow::avro::file::Compression;
use common_arrow::avro::read_async::block_stream;
use common_arrow::avro::read_async::read_metadata;
use common_arrow::avro::schema::Schema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_pipeline_core::Pipeline;
use common_settings::Settings;
use futures::Stream;
use futures::StreamExt;
use opendal::io_util::CompressAlgorithm;
use opendal::io_util::SeekableReader;
use opendal::Operator;

use crate::processors::sources::input_formats::delimiter::RecordDelimiter;
use crate::processors::sources::input_formats::input_context::CopyIntoPlan;
use crate::processors::sources::input_formats::input_pipeline::InputFormatPipe;
use crate::processors::sources::input_formats::input_split::DynData;
use crate::processors::sources::input_formats::input_split::FileInfo;
use crate::processors::sources::input_formats::input_split::SplitInfo;
use crate::processors::sources::input_formats::InputContext;
use crate::processors::sources::input_formats::InputFormat;

pub struct InputFormatAvro;

#[async_trait::async_trait]
impl InputFormat for InputFormatAvro {
    // not used
    fn get_format_settings(&self, settings: &Arc<Settings>) -> Result<FormatSettings> {
        Ok(FormatSettings::default())
    }
    // not used
    fn default_record_delimiter(&self) -> RecordDelimiter {
        RecordDelimiter::Crlf
    }
    // not used
    fn default_field_delimiter(&self) -> u8 {
        b'_'
    }

    async fn get_splits(
        &self,
        plan: &CopyIntoPlan,
        op: &Operator,
        settings: &Arc<Settings>,
        schema: &DataSchemaRef,
    ) -> Result<Vec<Arc<SplitInfo>>> {
        let mut infos = vec![];
        for path in &plan.files {
            let obj = op.object(path);
            let size = obj.metadata().await?.content_length() as usize;
            let mut reader: SeekableReader = obj.seekable_reader(..(size as u64));
            let mut file_meta = read_metadata(&mut reader)
                .await
                .map_err(|e| ErrorCode::AvroError(e.to_string()))?;

            // get the total size of AVRO header
            let mut offset = reader.stream_position().await;

            let mut blocks = block_stream(&mut reader, file_meta.marker).await;
            let mut blks = vec![];
            while let Some(block_res) = blocks.next().await {
                let block = block_res?;
                blks.push(block);
            }

            let fields = &file_meta.record.fields;
            let read_file_meta = Arc::new(FileMeta { fields });
            let file_info = Arc::new(FileInfo {
                path: path.clone(),
                size,
                num_splits: row_groups.len(),
                compress_alg: file_meta.compression.map(|c| match c {
                    Compression::Snappy => unimplemented!(),
                    Compression::Deflate => CompressAlgorithm::Deflate,
                }),
            });

            let num_file_splits = blks.len();
            for (idx, block) in blks.into_iter().enumerate() {
                if block.number_of_rows != 0 {
                    let size_in_bytes = block.data.len();
                    let block_meta = BlockMetadata {
                        rows: block.number_of_rows,
                        size_in_bytes,
                    };
                    let meta = Arc::new(SplitMeta {
                        file: read_file_meta.clone(),
                        meta: block_meta,
                    });
                    let info = Arc::new(SplitInfo {
                        file: file_info.clone(),
                        seq_in_file: idx,
                        offset,
                        size,
                        num_file_splits,
                        format_info: Some(meta),
                    });
                    infos.push(info);
                    offset += size;
                }
            }
        }
        Ok(infos)
    }

    fn exec_copy(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        AvroFormatPipe::execute_copy_aligned(ctx, pipeline)
    }

    fn exec_stream(&self, ctx: Arc<InputContext>, pipeline: &mut Pipeline) -> Result<()> {
        AvroFormatPipe::execute_stream(ctx, pipeline)
    }
}

pub struct AvroFormatPipe;

#[async_trait::async_trait]
impl InputFormatPipe for AvroFormatPipe {
    type SplitMeta = SplitMeta;
    type ReadBatch = Vec<u8>;
    type RowBatch = CompressedBlock;
    type AligningState = AligningState;
    type BlockBuilder = AvroBlockBuilder;

    async fn read_split(
        ctx: Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<Self::RowBatch> {
        let meta = Self::get_split_meta(split_info).expect("must success");
        let op = ctx.source.get_operator()?;
        let obj = op.object(&split_info.file.path);
        let mut reader = obj.seekable_reader(..(split_info.file.size as u64));
        BlockInMemory::read_async(&mut reader, meta.meta.clone(), meta.file.fields.clone()).await
    }
}

pub struct FileMeta {
    fields: Arc<Vec<Field>>,
}

pub struct SplitMeta {
    pub file: Arc<FileMeta>,
    pub meta: BlockMetadata,
}

impl DynData for SplitMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct BlockMetadata {
    rows: usize,
    size_in_bytes: usize,
}

pub struct AligningState {}
