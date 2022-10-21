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

use std::any::Any;
use std::io::Read;
use std::io::Seek;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk as ArrowChunk;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::io::avro::read;
use common_arrow::arrow::io::avro::read::deserialize;
use common_arrow::arrow::io::avro::read::Reader as AvroReader;
use common_arrow::avro::file::CompressedBlock;
use common_arrow::avro::file::Compression;
use common_arrow::avro::file::FileMetadata;
use common_arrow::avro::read::block_iterator;
use common_arrow::avro::read::fallible_streaming_iterator::FallibleStreamingIterator;
use common_arrow::avro::read::BlockStreamingIterator;
use common_arrow::avro::read_async::block_stream;
use common_arrow::avro::read_async::read_metadata;
use common_arrow::avro::schema::Field as AvroField;
use common_arrow::avro::schema::Schema;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_pipeline_core::Pipeline;
use common_settings::Settings;
use futures::AsyncSeek;
use futures::AsyncSeekExt;
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
                blks.push((offset, block));
                offset = reader.stream_position().await;
            }

            let fields = &file_meta.record.fields;
            let read_file_meta = Arc::new(FileMeta { fields });
            let compress_alg = file_meta.compression.map(|c| match c {
                Compression::Snappy => unimplemented!(),
                Compression::Deflate => CompressAlgorithm::Deflate,
            });

            let file_info = Arc::new(FileInfo {
                path: path.clone(),
                size,
                num_splits: blks.len(),
                compress_alg,
            });

            let num_file_splits = blks.len();
            for (idx, (offset, block)) in blks.into_iter().enumerate() {
                if block.number_of_rows != 0 {
                    let size_in_bytes = block.data.len();
                    let block_meta = Arc::new(file_meta);
                    let meta = Arc::new(SplitMeta { meta: block_meta });
                    let info = Arc::new(SplitInfo {
                        file: file_info.clone(),
                        seq_in_file: idx,
                        offset,
                        size,
                        num_file_splits,
                        format_info: Some(meta),
                    });
                    infos.push(info);
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
    type SplitMeta = FileMetadata;
    type ReadBatch = Vec<u8>;
    type RowBatch = ArrowChunk<Box<dyn Array>>;
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
        read_block_to_chunks(&mut reader, meta.meta.clone(), meta.file.fields.clone())
    }
}

pub struct FileMeta {
    fields: Arc<Vec<Field>>,
}

pub struct SplitMeta {
    pub meta: Arc<FileMetadata>,
}

impl DynData for SplitMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// use it if the header is read completely
fn read_block_to_chunks<R: Read + Seek>(
    reader: &mut R,
    meta: FileMetadata,
    fields: Arc<Vec<Field>>,
) -> Result<Vec<ArrowChunk<Box<dyn Array>>>> {
    let field_names = fields.iter().map(|x| x.name.as_str()).collect::<Vec<_>>();
    let block_iter: BlockStreamingIterator<R> =
        block_iterator(reader, meta.compression.clone(), meta.marker);

    let schema = read::infer_schema(&meta.record)?;
    let mut projections = vec![];
    let mut field_idx = 0;
    for field in schema.fields.iter() {
        if field.name == field_names[field_idx] {
            field_idx += 1;
            projections.push(true);
        } else {
            projections.push(false);
        }
    }
    let mut chunks = vec![];
    let r = AvroReader::new(reader, meta, schema.fields, Some(projections));
    // decompressed blocks
    for maybe_chrunks in r {
        let chunk = maybe_chrunks?;
        chunks.push(chunk);
    }
    Ok(chunks)
}

pub struct AligningState {}
