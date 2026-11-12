use toasty::stmt::Type;
use toasty_core::stmt::Value;
use toasty_core::Result;
use worker::SqlStorageValue;

pub(crate) enum SqlReturn {
    Count,
    Infer,
    Types(Vec<Type>),
}

/// Convert Toasty `Value` -> Cloudflare `SqlStorageValue`
pub(crate) fn toasty_to_cf_value(val: Value) -> Result<SqlStorageValue> {
    Ok(match val {
        Value::Bool(val) => SqlStorageValue::Boolean(val),
        Value::I8(val) => SqlStorageValue::Integer(val as i64),
        Value::I16(val) => SqlStorageValue::Integer(val as i64),
        Value::I32(val) => SqlStorageValue::Integer(val as i64),
        Value::I64(val) => SqlStorageValue::Integer(val),
        Value::U8(val) => SqlStorageValue::Integer(val as i64),
        Value::U16(val) => SqlStorageValue::Integer(val as i64),
        Value::U32(val) => SqlStorageValue::Integer(val as i64),
        Value::U64(val) => SqlStorageValue::Integer(val as i64),
        Value::F32(val) => SqlStorageValue::Float(val as f64),
        Value::F64(val) => SqlStorageValue::Float(val),
        Value::Null => SqlStorageValue::Null,
        Value::String(val) => SqlStorageValue::String(val),
        Value::Bytes(items) => SqlStorageValue::Blob(items),
        Value::Uuid(uuid) => SqlStorageValue::String(uuid.to_string()),
        Value::Timestamp(timestamp) => SqlStorageValue::String(timestamp.to_string()),
        Value::Zoned(zoned) => SqlStorageValue::String(zoned.to_string()),
        Value::Date(date) => SqlStorageValue::String(date.to_string()),
        Value::Time(time) => SqlStorageValue::String(time.to_string()),
        Value::DateTime(date_time) => SqlStorageValue::String(date_time.to_string()),
        _ => SqlStorageValue::String(value_to_json_text(&val)),
    })
}

/// Convert Cloudflare `SqlStorageValue` -> Toasty `Value`
pub(crate) fn cf_to_toasty_value(val: SqlStorageValue) -> Value {
    match val {
        SqlStorageValue::Null => Value::Null,
        SqlStorageValue::Integer(i) => Value::I64(i),
        SqlStorageValue::Float(f) => Value::F64(f),
        SqlStorageValue::String(s) => Value::String(s),
        SqlStorageValue::Blob(b) => Value::Bytes(b),
        SqlStorageValue::Boolean(b) => Value::Bool(b),
    }
}

pub(crate) fn cf_to_toasty_value_with_expected_type(value: SqlStorageValue, ty: &Type) -> Value {
    

    match value {
        SqlStorageValue::Boolean(val) => Value::Bool(val),
        SqlStorageValue::Null => Value::Null,
        SqlStorageValue::Integer(value) => match ty {
            Type::Bool => Value::Bool(value != 0),
            Type::I8 => Value::I8(value as i8),
            Type::I16 => Value::I16(value as i16),
            Type::I32 => Value::I32(value as i32),
            Type::I64 => Value::I64(value),
            Type::U8 => Value::U8(value as u8),
            Type::U16 => Value::U16(value as u16),
            Type::U32 => Value::U32(value as u32),
            Type::U64 => Value::U64(value as u64),
            _ => todo!("ty={ty:#?}"),
        },
        SqlStorageValue::Float(value) => match ty {
            Type::F32 => Value::F32(value as f32),
            Type::F64 => Value::F64(value),
            _ => todo!("ty={ty:#?}"),
        },
        SqlStorageValue::String(value) => match ty {
            Type::Uuid => Value::Uuid(value.parse().expect("text is a valid uuid")),
            Type::List(elem) => json_text_to_value_list(&value, elem),
            // A bare `#[document]` column (`Type::Object`) decodes
            // shape-directed to the named `Value::Object` wire form; the
            // engine raises it to the embed's positional record.
            Type::Object => json_text_to_value(&value, ty),
            _ => Value::String(value),
        },
        SqlStorageValue::Blob(value) => match ty {
            Type::Bytes => Value::Bytes(value),
            _ => todo!("value={value:#?}"),
        },
    }
}

pub(crate) fn value_to_json_text(value: &Value) -> String {
    toasty_sql::json::to_string(value).expect("serialize document value to JSON")
}

pub(crate) fn json_text_to_value_list(text: &str, elem_ty: &Type) -> Value {
    toasty_sql::json::list_from_str(text, elem_ty)
        .expect("SQLite returned non-JSON for a collection column")
}

pub(crate) fn json_text_to_value(text: &str, ty: &Type) -> Value {
    toasty_sql::json::from_str(text, ty).expect("SQLite returned non-JSON for a document column")
}
