use std::{
    io::{Cursor, Read},
    ops::Range,
};

use thiserror::Error;

use crate::{CompactSpanId, Field, LogRow, TraceBlock, TraceSpanRow};

const ROW_CODEC_VERSION: u8 = 1;
const ROW_BATCH_MAGIC: &[u8] = b"VTROWB1";
const ROW_BATCH_VERSION: u8 = 1;
const TRACE_BLOCK_MAGIC: &[u8] = b"VTBLKB1";
const TRACE_BLOCK_VERSION: u8 = 3;
const LOG_ROW_CODEC_VERSION: u8 = 1;
const LOG_ROW_BATCH_MAGIC: &[u8] = b"VTLOGB1";
const LOG_ROW_BATCH_VERSION: u8 = 1;
const COMPACT_SPAN_ID_TEXT_TAG: u8 = 0;
const COMPACT_SPAN_ID_HEX64_TAG: u8 = 1;
const OPTIONAL_COMPACT_SPAN_ID_NONE_TAG: u8 = 0;
const OPTIONAL_COMPACT_SPAN_ID_TEXT_TAG: u8 = 1;
const OPTIONAL_COMPACT_SPAN_ID_HEX64_TAG: u8 = 2;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

#[derive(Debug, Error)]
pub enum RowCodecError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid row codec version {0}")]
    InvalidVersion(u8),
    #[error("invalid batch header")]
    InvalidBatchHeader,
    #[error("invalid utf-8 string: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
}

struct EncodedSharedFieldGroup {
    field_count: u32,
    encoded_fields: Vec<u8>,
}

pub fn encode_trace_row(row: &TraceSpanRow) -> Vec<u8> {
    let mut output = Vec::with_capacity(256 + row.fields.len() * 32);
    output.push(ROW_CODEC_VERSION);
    write_string(&mut output, &row.trace_id);
    write_string(&mut output, &row.span_id);
    write_optional_string(&mut output, row.parent_span_id.as_deref());
    write_string(&mut output, &row.name);
    write_i64(&mut output, row.start_unix_nano);
    write_i64(&mut output, row.end_unix_nano);
    write_i64(&mut output, row.time_unix_nano);
    write_u32(&mut output, row.fields.len() as u32);
    for field in &row.fields {
        write_string(&mut output, &field.name);
        write_string(&mut output, &field.value);
    }
    output
}

pub fn decode_trace_row(bytes: &[u8]) -> Result<TraceSpanRow, RowCodecError> {
    let mut cursor = Cursor::new(bytes);
    let version = read_u8(&mut cursor)?;
    if version != ROW_CODEC_VERSION {
        return Err(RowCodecError::InvalidVersion(version));
    }

    let trace_id = read_string(&mut cursor)?;
    let span_id = read_string(&mut cursor)?;
    let parent_span_id = read_optional_string(&mut cursor)?;
    let name = read_string(&mut cursor)?;
    let start_unix_nano = read_i64(&mut cursor)?;
    let end_unix_nano = read_i64(&mut cursor)?;
    let time_unix_nano = read_i64(&mut cursor)?;
    let field_count = read_u32(&mut cursor)? as usize;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        fields.push(Field {
            name: read_string(&mut cursor)?.into(),
            value: read_string(&mut cursor)?.into(),
        });
    }

    Ok(TraceSpanRow {
        trace_id,
        span_id,
        parent_span_id,
        name,
        start_unix_nano,
        end_unix_nano,
        time_unix_nano,
        fields,
    })
}

pub fn encode_trace_rows(rows: &[TraceSpanRow]) -> Vec<u8> {
    encode_trace_rows_from_encoded_rows(rows.iter().map(encode_trace_row))
}

pub fn encode_trace_block_row(block: &TraceBlock, row_index: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(256 + block.fields_at(row_index).len() * 32);
    encode_trace_block_row_into(&mut output, block, row_index);
    output
}

pub fn encode_trace_block_rows_packed(block: &TraceBlock) -> (Vec<u8>, Vec<Range<usize>>) {
    let mut bytes = Vec::with_capacity(
        256 * block.row_count() + (block.shared_fields.len() + block.fields.len()) * 32,
    );
    let mut ranges = Vec::with_capacity(block.row_count());
    let shared_group_encodings = encode_shared_field_groups(block);
    for row_index in 0..block.row_count() {
        let start = bytes.len();
        bytes.push(ROW_CODEC_VERSION);
        write_string(&mut bytes, block.trace_id_at(row_index));
        write_compact_span_id_as_string(&mut bytes, &block.span_ids[row_index]);
        write_optional_compact_span_id_as_string(
            &mut bytes,
            block.parent_span_ids[row_index].as_ref(),
        );
        write_string(&mut bytes, block.name_at(row_index));
        write_i64(&mut bytes, block.start_unix_nano_at(row_index));
        write_i64(&mut bytes, block.end_unix_nano_at(row_index));
        write_i64(&mut bytes, block.time_unix_nano_at(row_index));
        let shared_group = block
            .shared_field_group_id_at(row_index)
            .checked_sub(1)
            .and_then(|group_index| shared_group_encodings.get(group_index as usize));
        let row_fields = block.row_fields_at(row_index);
        write_u32(
            &mut bytes,
            shared_group.map_or(0, |group| group.field_count) + row_fields.len() as u32,
        );
        if let Some(shared_group) = shared_group {
            bytes.extend_from_slice(&shared_group.encoded_fields);
        }
        for field in row_fields {
            write_string(&mut bytes, &field.name);
            write_string(&mut bytes, &field.value);
        }
        ranges.push(start..bytes.len());
    }
    (bytes, ranges)
}

pub fn encode_trace_block(block: &TraceBlock) -> Vec<u8> {
    let mut output =
        Vec::with_capacity(256 + (block.shared_fields.len() + block.fields.len()) * 24);
    output.extend_from_slice(TRACE_BLOCK_MAGIC);
    output.push(TRACE_BLOCK_VERSION);
    write_u32(&mut output, block.row_count() as u32);

    for values in [&block.trace_ids, &block.names] {
        write_u32(&mut output, values.len() as u32);
        for value in values.iter() {
            write_string(&mut output, value.as_ref());
        }
    }

    write_u32(&mut output, block.span_ids.len() as u32);
    for value in &block.span_ids {
        write_compact_span_id(&mut output, value);
    }

    write_u32(&mut output, block.parent_span_ids.len() as u32);
    for value in &block.parent_span_ids {
        write_optional_compact_span_id(&mut output, value.as_ref());
    }

    for values in [
        &block.start_unix_nanos,
        &block.end_unix_nanos,
        &block.time_unix_nanos,
    ] {
        write_u32(&mut output, values.len() as u32);
        for value in values.iter() {
            write_i64(&mut output, *value);
        }
    }

    write_u32(&mut output, block.shared_field_group_ids.len() as u32);
    for group_id in &block.shared_field_group_ids {
        write_u32(&mut output, *group_id);
    }

    write_u32(&mut output, block.shared_field_offsets.len() as u32);
    for offset in &block.shared_field_offsets {
        write_u32(&mut output, *offset);
    }

    write_u32(&mut output, block.shared_fields.len() as u32);
    for field in &block.shared_fields {
        write_string(&mut output, &field.name);
        write_string(&mut output, &field.value);
    }

    write_u32(&mut output, block.field_offsets.len() as u32);
    for offset in &block.field_offsets {
        write_u32(&mut output, *offset);
    }

    write_u32(&mut output, block.fields.len() as u32);
    for field in &block.fields {
        write_string(&mut output, &field.name);
        write_string(&mut output, &field.value);
    }

    output
}

pub fn decode_trace_block(bytes: &[u8]) -> Result<TraceBlock, RowCodecError> {
    if bytes.len() < TRACE_BLOCK_MAGIC.len() + 1 + 4 {
        return Err(RowCodecError::InvalidBatchHeader);
    }

    let mut cursor = Cursor::new(bytes);
    let mut magic = vec![0u8; TRACE_BLOCK_MAGIC.len()];
    cursor
        .read_exact(&mut magic)
        .map_err(map_read_error_to_codec_error)?;
    if magic != TRACE_BLOCK_MAGIC {
        return Err(RowCodecError::InvalidBatchHeader);
    }

    let version = read_u8(&mut cursor)?;
    if version != TRACE_BLOCK_VERSION {
        return Err(RowCodecError::InvalidVersion(version));
    }

    let row_count = read_u32(&mut cursor)? as usize;
    let trace_ids = read_string_vec(&mut cursor)?;
    let names = read_string_vec(&mut cursor)?;
    let span_ids = read_compact_span_id_vec(&mut cursor)?;
    let parent_span_ids = read_optional_compact_span_id_vec(&mut cursor)?;
    let start_unix_nanos = read_i64_vec(&mut cursor)?;
    let end_unix_nanos = read_i64_vec(&mut cursor)?;
    let time_unix_nanos = read_i64_vec(&mut cursor)?;
    let shared_field_group_ids = read_u32_vec(&mut cursor)?;
    let shared_field_offsets = read_u32_vec(&mut cursor)?;
    let shared_fields = read_fields(&mut cursor)?;
    let field_offsets = read_u32_vec(&mut cursor)?;
    let fields = read_fields(&mut cursor)?;

    let block = TraceBlock {
        trace_ids: trace_ids.into_iter().map(Into::into).collect(),
        span_ids,
        parent_span_ids,
        names: names.into_iter().map(Into::into).collect(),
        start_unix_nanos,
        end_unix_nanos,
        time_unix_nanos,
        shared_field_group_ids,
        shared_field_offsets,
        shared_fields,
        field_offsets,
        fields,
    };

    if block.row_count() != row_count {
        return Err(RowCodecError::InvalidBatchHeader);
    }
    Ok(block)
}

pub fn encode_trace_rows_from_encoded_rows<'a, I, B>(encoded_rows: I) -> Vec<u8>
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    let encoded_rows: Vec<B> = encoded_rows.into_iter().collect();
    let estimated_payload_bytes: usize = encoded_rows
        .iter()
        .map(|encoded_row| encoded_row.as_ref().len() + 4)
        .sum();
    let mut output = Vec::with_capacity(ROW_BATCH_MAGIC.len() + 1 + 4 + estimated_payload_bytes);
    output.extend_from_slice(ROW_BATCH_MAGIC);
    output.push(ROW_BATCH_VERSION);
    write_u32(&mut output, encoded_rows.len() as u32);
    for encoded_row in encoded_rows {
        let encoded_row = encoded_row.as_ref();
        write_u32(&mut output, encoded_row.len() as u32);
        output.extend_from_slice(encoded_row);
    }
    output
}

fn encode_trace_block_row_into(output: &mut Vec<u8>, block: &TraceBlock, row_index: usize) {
    output.push(ROW_CODEC_VERSION);
    write_string(output, block.trace_id_at(row_index));
    write_compact_span_id_as_string(output, &block.span_ids[row_index]);
    write_optional_compact_span_id_as_string(output, block.parent_span_ids[row_index].as_ref());
    write_string(output, block.name_at(row_index));
    write_i64(output, block.start_unix_nano_at(row_index));
    write_i64(output, block.end_unix_nano_at(row_index));
    write_i64(output, block.time_unix_nano_at(row_index));
    let fields = block.fields_at(row_index);
    write_u32(output, fields.len() as u32);
    for field in fields {
        write_string(output, &field.name);
        write_string(output, &field.value);
    }
}

fn encode_shared_field_groups(block: &TraceBlock) -> Vec<EncodedSharedFieldGroup> {
    let mut encoded_groups = Vec::with_capacity(block.shared_field_group_count());
    for group_id in 1..=block.shared_field_group_count() as u32 {
        let shared_fields = block.shared_fields_for_group_id(group_id);
        let mut encoded_fields = Vec::with_capacity(shared_fields.len() * 32);
        for field in shared_fields {
            write_string(&mut encoded_fields, &field.name);
            write_string(&mut encoded_fields, &field.value);
        }
        encoded_groups.push(EncodedSharedFieldGroup {
            field_count: shared_fields.len() as u32,
            encoded_fields,
        });
    }
    encoded_groups
}

pub fn decode_trace_rows(bytes: &[u8]) -> Result<Vec<TraceSpanRow>, RowCodecError> {
    if bytes.len() < ROW_BATCH_MAGIC.len() + 1 + 4 {
        return Err(RowCodecError::InvalidBatchHeader);
    }
    let mut cursor = Cursor::new(bytes);
    let mut magic = vec![0u8; ROW_BATCH_MAGIC.len()];
    cursor
        .read_exact(&mut magic)
        .map_err(map_read_error_to_codec_error)?;
    if magic != ROW_BATCH_MAGIC {
        return Err(RowCodecError::InvalidBatchHeader);
    }
    let version = read_u8(&mut cursor)?;
    if version != ROW_BATCH_VERSION {
        return Err(RowCodecError::InvalidVersion(version));
    }
    let row_count = read_u32(&mut cursor)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let row_len = read_u32(&mut cursor)? as usize;
        let start = cursor.position() as usize;
        let end = start.saturating_add(row_len);
        let row_bytes = bytes.get(start..end).ok_or(RowCodecError::UnexpectedEof)?;
        rows.push(decode_trace_row(row_bytes)?);
        cursor.set_position(end as u64);
    }
    Ok(rows)
}

pub fn encode_log_row(row: &LogRow) -> Vec<u8> {
    let mut output = Vec::with_capacity(256 + row.fields.len() * 32);
    output.push(LOG_ROW_CODEC_VERSION);
    write_string(&mut output, &row.log_id);
    write_i64(&mut output, row.time_unix_nano);
    write_optional_i64(&mut output, row.observed_time_unix_nano);
    write_optional_i32(&mut output, row.severity_number);
    write_optional_string(&mut output, row.severity_text.as_deref());
    write_string(&mut output, &row.body);
    write_optional_string(&mut output, row.trace_id.as_deref());
    write_optional_string(&mut output, row.span_id.as_deref());
    write_u32(&mut output, row.fields.len() as u32);
    for field in &row.fields {
        write_string(&mut output, &field.name);
        write_string(&mut output, &field.value);
    }
    output
}

pub fn decode_log_row(bytes: &[u8]) -> Result<LogRow, RowCodecError> {
    let mut cursor = Cursor::new(bytes);
    let version = read_u8(&mut cursor)?;
    if version != LOG_ROW_CODEC_VERSION {
        return Err(RowCodecError::InvalidVersion(version));
    }

    let log_id = read_string(&mut cursor)?;
    let time_unix_nano = read_i64(&mut cursor)?;
    let observed_time_unix_nano = read_optional_i64(&mut cursor)?;
    let severity_number = read_optional_i32(&mut cursor)?;
    let severity_text = read_optional_string(&mut cursor)?;
    let body = read_string(&mut cursor)?;
    let trace_id = read_optional_string(&mut cursor)?;
    let span_id = read_optional_string(&mut cursor)?;
    let field_count = read_u32(&mut cursor)? as usize;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        fields.push(Field {
            name: read_string(&mut cursor)?.into(),
            value: read_string(&mut cursor)?.into(),
        });
    }

    Ok(LogRow::new_unsorted_fields(
        log_id,
        time_unix_nano,
        observed_time_unix_nano,
        severity_number,
        severity_text,
        body,
        trace_id,
        span_id,
        fields,
    ))
}

pub fn encode_log_rows(rows: &[LogRow]) -> Vec<u8> {
    encode_log_rows_from_encoded_rows(rows.iter().map(encode_log_row))
}

pub fn encode_log_rows_from_encoded_rows<'a, I, B>(encoded_rows: I) -> Vec<u8>
where
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    let encoded_rows: Vec<B> = encoded_rows.into_iter().collect();
    let estimated_payload_bytes: usize = encoded_rows
        .iter()
        .map(|encoded_row| encoded_row.as_ref().len() + 4)
        .sum();
    let mut output =
        Vec::with_capacity(LOG_ROW_BATCH_MAGIC.len() + 1 + 4 + estimated_payload_bytes);
    output.extend_from_slice(LOG_ROW_BATCH_MAGIC);
    output.push(LOG_ROW_BATCH_VERSION);
    write_u32(&mut output, encoded_rows.len() as u32);
    for encoded_row in encoded_rows {
        let encoded_row = encoded_row.as_ref();
        write_u32(&mut output, encoded_row.len() as u32);
        output.extend_from_slice(encoded_row);
    }
    output
}

pub fn decode_log_rows(bytes: &[u8]) -> Result<Vec<LogRow>, RowCodecError> {
    if bytes.len() < LOG_ROW_BATCH_MAGIC.len() + 1 + 4 {
        return Err(RowCodecError::InvalidBatchHeader);
    }
    let mut cursor = Cursor::new(bytes);
    let mut magic = vec![0u8; LOG_ROW_BATCH_MAGIC.len()];
    cursor
        .read_exact(&mut magic)
        .map_err(map_read_error_to_codec_error)?;
    if magic != LOG_ROW_BATCH_MAGIC {
        return Err(RowCodecError::InvalidBatchHeader);
    }
    let version = read_u8(&mut cursor)?;
    if version != LOG_ROW_BATCH_VERSION {
        return Err(RowCodecError::InvalidVersion(version));
    }
    let row_count = read_u32(&mut cursor)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let row_len = read_u32(&mut cursor)? as usize;
        let start = cursor.position() as usize;
        let end = start.saturating_add(row_len);
        let row_bytes = bytes.get(start..end).ok_or(RowCodecError::UnexpectedEof)?;
        rows.push(decode_log_row(row_bytes)?);
        cursor.set_position(end as u64);
    }
    Ok(rows)
}

fn write_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_i32(output: &mut Vec<u8>, value: i32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_string(output: &mut Vec<u8>, value: &str) {
    write_u32(output, value.len() as u32);
    output.extend_from_slice(value.as_bytes());
}

fn write_optional_string(output: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            output.push(1);
            write_string(output, value);
        }
        None => output.push(0),
    }
}

fn write_compact_span_id(output: &mut Vec<u8>, value: &CompactSpanId) {
    match value {
        CompactSpanId::Text(value) => {
            output.push(COMPACT_SPAN_ID_TEXT_TAG);
            write_string(output, value);
        }
        CompactSpanId::Hex64(value) => {
            output.push(COMPACT_SPAN_ID_HEX64_TAG);
            write_u64(output, *value);
        }
    }
}

fn write_optional_compact_span_id(output: &mut Vec<u8>, value: Option<&CompactSpanId>) {
    match value {
        None => output.push(OPTIONAL_COMPACT_SPAN_ID_NONE_TAG),
        Some(CompactSpanId::Text(value)) => {
            output.push(OPTIONAL_COMPACT_SPAN_ID_TEXT_TAG);
            write_string(output, value);
        }
        Some(CompactSpanId::Hex64(value)) => {
            output.push(OPTIONAL_COMPACT_SPAN_ID_HEX64_TAG);
            write_u64(output, *value);
        }
    }
}

fn write_compact_span_id_as_string(output: &mut Vec<u8>, value: &CompactSpanId) {
    match value {
        CompactSpanId::Text(value) => write_string(output, value),
        CompactSpanId::Hex64(value) => {
            write_u32(output, 16);
            append_hex_u64(output, *value);
        }
    }
}

fn write_optional_compact_span_id_as_string(output: &mut Vec<u8>, value: Option<&CompactSpanId>) {
    match value {
        Some(value) => {
            output.push(1);
            write_compact_span_id_as_string(output, value);
        }
        None => output.push(0),
    }
}

fn write_optional_i64(output: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            output.push(1);
            write_i64(output, value);
        }
        None => output.push(0),
    }
}

fn write_optional_i32(output: &mut Vec<u8>, value: Option<i32>) {
    match value {
        Some(value) => {
            output.push(1);
            write_i32(output, value);
        }
        None => output.push(0),
    }
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, RowCodecError> {
    let mut bytes = [0u8; 1];
    cursor
        .read_exact(&mut bytes)
        .map_err(map_read_error_to_codec_error)?;
    Ok(bytes[0])
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, RowCodecError> {
    let mut bytes = [0u8; 4];
    cursor
        .read_exact(&mut bytes)
        .map_err(map_read_error_to_codec_error)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_i64(cursor: &mut Cursor<&[u8]>) -> Result<i64, RowCodecError> {
    let mut bytes = [0u8; 8];
    cursor
        .read_exact(&mut bytes)
        .map_err(map_read_error_to_codec_error)?;
    Ok(i64::from_le_bytes(bytes))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64, RowCodecError> {
    let mut bytes = [0u8; 8];
    cursor
        .read_exact(&mut bytes)
        .map_err(map_read_error_to_codec_error)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_i32(cursor: &mut Cursor<&[u8]>) -> Result<i32, RowCodecError> {
    let mut bytes = [0u8; 4];
    cursor
        .read_exact(&mut bytes)
        .map_err(map_read_error_to_codec_error)?;
    Ok(i32::from_le_bytes(bytes))
}

fn read_string(cursor: &mut Cursor<&[u8]>) -> Result<String, RowCodecError> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0u8; len];
    cursor
        .read_exact(&mut bytes)
        .map_err(map_read_error_to_codec_error)?;
    Ok(String::from_utf8(bytes)?)
}

fn read_optional_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>, RowCodecError> {
    match read_u8(cursor)? {
        0 => Ok(None),
        1 => Ok(Some(read_string(cursor)?)),
        other => Err(RowCodecError::InvalidVersion(other)),
    }
}

fn read_optional_i64(cursor: &mut Cursor<&[u8]>) -> Result<Option<i64>, RowCodecError> {
    match read_u8(cursor)? {
        0 => Ok(None),
        1 => Ok(Some(read_i64(cursor)?)),
        other => Err(RowCodecError::InvalidVersion(other)),
    }
}

fn read_optional_i32(cursor: &mut Cursor<&[u8]>) -> Result<Option<i32>, RowCodecError> {
    match read_u8(cursor)? {
        0 => Ok(None),
        1 => Ok(Some(read_i32(cursor)?)),
        other => Err(RowCodecError::InvalidVersion(other)),
    }
}

fn read_string_vec(cursor: &mut Cursor<&[u8]>) -> Result<Vec<String>, RowCodecError> {
    let len = read_u32(cursor)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_string(cursor)?);
    }
    Ok(values)
}

fn read_compact_span_id(cursor: &mut Cursor<&[u8]>) -> Result<CompactSpanId, RowCodecError> {
    match read_u8(cursor)? {
        COMPACT_SPAN_ID_TEXT_TAG => Ok(CompactSpanId::Text(read_string(cursor)?.into())),
        COMPACT_SPAN_ID_HEX64_TAG => Ok(CompactSpanId::Hex64(read_u64(cursor)?)),
        other => Err(RowCodecError::InvalidVersion(other)),
    }
}

fn read_compact_span_id_vec(
    cursor: &mut Cursor<&[u8]>,
) -> Result<Vec<CompactSpanId>, RowCodecError> {
    let len = read_u32(cursor)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_compact_span_id(cursor)?);
    }
    Ok(values)
}

fn read_optional_compact_span_id(
    cursor: &mut Cursor<&[u8]>,
) -> Result<Option<CompactSpanId>, RowCodecError> {
    match read_u8(cursor)? {
        OPTIONAL_COMPACT_SPAN_ID_NONE_TAG => Ok(None),
        OPTIONAL_COMPACT_SPAN_ID_TEXT_TAG => {
            Ok(Some(CompactSpanId::Text(read_string(cursor)?.into())))
        }
        OPTIONAL_COMPACT_SPAN_ID_HEX64_TAG => Ok(Some(CompactSpanId::Hex64(read_u64(cursor)?))),
        other => Err(RowCodecError::InvalidVersion(other)),
    }
}

fn read_optional_compact_span_id_vec(
    cursor: &mut Cursor<&[u8]>,
) -> Result<Vec<Option<CompactSpanId>>, RowCodecError> {
    let len = read_u32(cursor)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_optional_compact_span_id(cursor)?);
    }
    Ok(values)
}

fn read_i64_vec(cursor: &mut Cursor<&[u8]>) -> Result<Vec<i64>, RowCodecError> {
    let len = read_u32(cursor)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_i64(cursor)?);
    }
    Ok(values)
}

fn read_u32_vec(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u32>, RowCodecError> {
    let len = read_u32(cursor)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_u32(cursor)?);
    }
    Ok(values)
}

fn read_fields(cursor: &mut Cursor<&[u8]>) -> Result<Vec<Field>, RowCodecError> {
    let len = read_u32(cursor)? as usize;
    let mut fields = Vec::with_capacity(len);
    for _ in 0..len {
        fields.push(Field {
            name: read_string(cursor)?.into(),
            value: read_string(cursor)?.into(),
        });
    }
    Ok(fields)
}

fn map_read_error_to_codec_error(error: std::io::Error) -> RowCodecError {
    match error.kind() {
        std::io::ErrorKind::UnexpectedEof => RowCodecError::UnexpectedEof,
        _ => RowCodecError::UnexpectedEof,
    }
}

fn append_hex_u64(output: &mut Vec<u8>, value: u64) {
    for shift in (0..16).rev() {
        let nibble = ((value >> (shift * 4)) & 0x0f) as usize;
        output.push(HEX_DIGITS[nibble]);
    }
}
