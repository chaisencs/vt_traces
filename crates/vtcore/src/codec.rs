use std::io::{Cursor, Read};

use thiserror::Error;

use crate::{Field, LogRow, TraceSpanRow};

const ROW_CODEC_VERSION: u8 = 1;
const ROW_BATCH_MAGIC: &[u8] = b"VTROWB1";
const ROW_BATCH_VERSION: u8 = 1;
const LOG_ROW_CODEC_VERSION: u8 = 1;
const LOG_ROW_BATCH_MAGIC: &[u8] = b"VTLOGB1";
const LOG_ROW_BATCH_VERSION: u8 = 1;

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

fn map_read_error_to_codec_error(error: std::io::Error) -> RowCodecError {
    match error.kind() {
        std::io::ErrorKind::UnexpectedEof => RowCodecError::UnexpectedEof,
        _ => RowCodecError::UnexpectedEof,
    }
}
