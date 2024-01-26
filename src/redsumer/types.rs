use bytes::Bytes;
use redis::{from_redis_value, ErrorKind, RedisError, RedisResult, Value};
use serde::de::DeserializeOwned;
use serde_json::from_str as json_from_str;
use std::fmt::Debug;
use std::str::FromStr;
use time::parsing::Parsable;
use time::{
    format_description::well_known::{Iso8601, Rfc2822, Rfc3339},
    Date, OffsetDateTime,
};
use uuid::Uuid;

/// Method handler to unwrap [`Value`] into an instance of multiple usable structs.
pub struct FromRedisValueImplHandler;

impl FromRedisValueImplHandler {
    /// Unwrap [`Value`] as a [`i8`].
    pub fn to_i8(&self, v: &Value) -> RedisResult<i8> {
        from_redis_value::<i8>(v)
    }

    /// Unwrap [`Value`] as an optional [`i8`].
    pub fn to_optional_i8(&self, v: &Value) -> RedisResult<Option<i8>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_i8(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`i16`].
    pub fn to_i16(&self, v: &Value) -> RedisResult<i16> {
        from_redis_value::<i16>(v)
    }

    /// Unwrap [`Value`] as an optional [`i16`].
    pub fn to_optional_i16(&self, v: &Value) -> RedisResult<Option<i16>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_i16(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`i32`].
    pub fn to_i32(&self, v: &Value) -> RedisResult<i32> {
        from_redis_value::<i32>(v)
    }

    /// Unwrap [`Value`] as an optional [`i32`].
    pub fn to_optional_i32(&self, v: &Value) -> RedisResult<Option<i32>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_i32(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`i64`].
    pub fn to_i64(&self, v: &Value) -> RedisResult<i64> {
        from_redis_value::<i64>(v)
    }

    /// Unwrap [`Value`] as an optional [`i64`].
    pub fn to_optional_i64(&self, v: &Value) -> RedisResult<Option<i64>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_i64(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`i128`].
    pub fn to_i128(&self, v: &Value) -> RedisResult<i128> {
        from_redis_value::<i128>(v)
    }

    /// Unwrap [`Value`] as an optional [`i128`].
    pub fn to_optional_i128(&self, v: &Value) -> RedisResult<Option<i128>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_i128(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`u8`].
    pub fn to_u8(&self, v: &Value) -> RedisResult<u8> {
        from_redis_value::<u8>(v)
    }

    /// Unwrap [`Value`] as an optional [`u8`].
    pub fn to_optional_u8(&self, v: &Value) -> RedisResult<Option<u8>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_u8(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`u16`].
    pub fn to_u16(&self, v: &Value) -> RedisResult<u16> {
        from_redis_value::<u16>(v)
    }

    /// Unwrap [`Value`] as an optional [`u16`].
    pub fn to_optional_u16(&self, v: &Value) -> RedisResult<Option<u16>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_u16(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`u32`].
    pub fn to_u32(&self, v: &Value) -> RedisResult<u32> {
        from_redis_value::<u32>(v)
    }

    /// Unwrap [`Value`] as an optional [`u32`].
    pub fn to_optional_u32(&self, v: &Value) -> RedisResult<Option<u32>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_u32(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`u64`].
    pub fn to_u64(&self, v: &Value) -> RedisResult<u64> {
        from_redis_value::<u64>(v)
    }

    /// Unwrap [`Value`] as an optional [`u64`].
    pub fn to_optional_u64(&self, v: &Value) -> RedisResult<Option<u64>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_u64(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`u128`].
    pub fn to_u128(&self, v: &Value) -> RedisResult<u128> {
        from_redis_value::<u128>(v)
    }

    /// Unwrap [`Value`] as an optional [`u128`].
    pub fn to_optional_u128(&self, v: &Value) -> RedisResult<Option<u128>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_u128(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`usize`].
    pub fn to_usize(&self, v: &Value) -> RedisResult<usize> {
        from_redis_value::<usize>(v)
    }

    /// Unwrap [`Value`] as an optional [`usize`].
    pub fn to_optional_usize(&self, v: &Value) -> RedisResult<Option<usize>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_usize(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`isize`].
    pub fn to_isize(&self, v: &Value) -> RedisResult<isize> {
        from_redis_value::<isize>(v)
    }

    /// Unwrap [`Value`] as an optional [`isize`].
    pub fn to_optional_isize(&self, v: &Value) -> RedisResult<Option<isize>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_isize(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`f32`].
    pub fn to_f32(&self, v: &Value) -> RedisResult<f32> {
        from_redis_value::<f32>(v)
    }

    /// Unwrap [`Value`] as an optional [`f32`].
    pub fn to_optional_f32(&self, v: &Value) -> RedisResult<Option<f32>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_f32(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`f64`].
    pub fn to_f64(&self, v: &Value) -> RedisResult<f64> {
        from_redis_value::<f64>(v)
    }

    /// Unwrap [`Value`] as an optional [`f64`].
    pub fn to_optional_f64(&self, v: &Value) -> RedisResult<Option<f64>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_f64(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`String`].
    pub fn to_string(&self, v: &Value) -> RedisResult<String> {
        from_redis_value::<String>(v)
    }

    /// Unwrap [`Value`] as an optional [`String`].
    pub fn to_optional_string(&self, v: &Value) -> RedisResult<Option<String>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_string(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`bool`].
    pub fn to_bool(&self, v: &Value) -> RedisResult<bool> {
        from_redis_value::<bool>(v)
    }

    /// Unwrap [`Value`] as an optional [`bool`].
    pub fn to_optional_bool(&self, v: &Value) -> RedisResult<Option<bool>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_bool(v)?))
        }
    }

    /// Unwrap [`Value`] as a [`Uuid`].
    pub fn to_uuid(&self, v: &Value) -> RedisResult<Uuid> {
        match Uuid::from_str(&self.to_string(v)?) {
            Ok(uuid) => Ok(uuid),
            Err(error) => Err(RedisError::from((
                ErrorKind::TypeError,
                "Response was of incompatible type",
                format!(
                    "Value {:?} is not parsable as Uuid: {:?}",
                    v,
                    &error.to_string(),
                ),
            ))),
        }
    }

    /// Unwrap [`Value`] as an optional [`Uuid`].
    pub fn to_optional_uuid(&self, v: &Value) -> RedisResult<Option<Uuid>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_uuid(v)?))
        }
    }

    /// Unwrap [`Value`] as [`OffsetDateTime`] with format `F`.
    pub fn to_offsetdatetime<F: Parsable + ?Sized + Debug>(
        &self,
        v: &Value,
        format: &F,
    ) -> RedisResult<OffsetDateTime> {
        match OffsetDateTime::parse(&from_redis_value::<String>(v)?, format) {
            Ok(offsetdatetime) => Ok(offsetdatetime),
            Err(error) => Err(RedisError::from((
                ErrorKind::TypeError,
                "Response was of incompatible type",
                format!(
                    "Value {:?} is not parsable as OffsetDatetime with specified format {:?}: {:?}",
                    v,
                    format,
                    &error.to_string(),
                ),
            ))),
        }
    }

    /// Unwrap [`Value`] as an optional [`OffsetDateTime`] with format `F`.
    pub fn to_optional_offsetdatetime<F: Parsable + ?Sized + Debug>(
        &self,
        v: &Value,
        format: &F,
    ) -> RedisResult<Option<OffsetDateTime>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_offsetdatetime(v, format)?))
        }
    }

    /// Unwrap [`Value`] as [`OffsetDateTime`] with specific format [`Iso8601`].
    pub fn to_offsetdatetime_from_iso8601(&self, v: &Value) -> RedisResult<OffsetDateTime> {
        self.to_offsetdatetime(v, &Iso8601::DEFAULT)
    }

    /// Unwrap [`Value`] as an optional [`OffsetDateTime`] with specific format [`Iso8601`].
    pub fn to_optional_offsetdatetime_from_iso8601(
        &self,
        v: &Value,
    ) -> RedisResult<Option<OffsetDateTime>> {
        self.to_optional_offsetdatetime(v, &Iso8601::DEFAULT)
    }

    /// Unwrap [`Value`] as [`OffsetDateTime`] with specific format [`Rfc2822`].
    pub fn to_offsetdatetime_from_rfc2822(&self, v: &Value) -> RedisResult<OffsetDateTime> {
        self.to_offsetdatetime(v, &Rfc2822)
    }

    /// Unwrap [`Value`] as an optional [`OffsetDateTime`] with specific format [`Rfc2822`].
    pub fn to_optional_offsetdatetime_from_rfc2822(
        &self,
        v: &Value,
    ) -> RedisResult<Option<OffsetDateTime>> {
        self.to_optional_offsetdatetime(v, &Rfc2822)
    }

    /// Unwrap [`Value`] as [`OffsetDateTime`] with specific format [`Rfc3339`].
    pub fn to_offsetdatetime_from_rfc3339(&self, v: &Value) -> RedisResult<OffsetDateTime> {
        self.to_offsetdatetime(v, &Rfc3339)
    }

    /// Unwrap [`Value`] as an optional [`OffsetDateTime`] with specific format [`Rfc3339`].
    pub fn to_optional_offsetdatetime_from_rfc3339(
        &self,
        v: &Value,
    ) -> RedisResult<Option<OffsetDateTime>> {
        self.to_optional_offsetdatetime(v, &Rfc3339)
    }

    /// Unwrap [`Value`] as [`Date`] with format `F`.
    pub fn to_date<F: Parsable + ?Sized + Debug>(
        &self,
        v: &Value,
        format: &F,
    ) -> RedisResult<Date> {
        match Date::parse(&from_redis_value::<String>(v)?, format) {
            Ok(date) => Ok(date),
            Err(error) => Err(RedisError::from((
                ErrorKind::TypeError,
                "Response was of incompatible type",
                format!(
                    "Value {:?} is not parsable as Date with specified format {:?}: {:?}",
                    v,
                    format,
                    &error.to_string(),
                ),
            ))),
        }
    }

    /// Unwrap [`Value`] as an optional [`Date`] with format `F`.
    pub fn to_optional_date<F: Parsable + ?Sized + Debug>(
        &self,
        v: &Value,
        format: &F,
    ) -> RedisResult<Option<Date>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_date(v, format)?))
        }
    }

    /// Unwrap [`Value`] as [`Date`] with specific format [`Iso8601`].
    pub fn to_date_from_iso8601(&self, v: &Value) -> RedisResult<Date> {
        self.to_date(v, &Iso8601::DEFAULT)
    }

    /// Unwrap [`Value`] as an optional [`Date`] with specific format [`Iso8601`].
    pub fn to_optional_date_from_iso8601(&self, v: &Value) -> RedisResult<Option<Date>> {
        self.to_optional_date(v, &Iso8601::DEFAULT)
    }

    /// Unwrap [`Value`] as [`Bytes`].
    pub fn to_bytes(&self, v: &Value) -> RedisResult<Bytes> {
        from_redis_value::<Bytes>(v)
    }

    /// Unwrap [`Value`] as an optional [`Bytes`].
    pub fn to_optional_bytes(&self, v: &Value) -> RedisResult<Option<Bytes>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_bytes(v)?))
        }
    }

    /// Unwrap [`Value`] as an instance of generic struct `S`.
    pub fn to_struct_instance<S: DeserializeOwned>(&self, v: &Value) -> RedisResult<S> {
        match json_from_str::<S>(&from_redis_value::<String>(v)?) {
            Ok(obj) => Ok(obj),
            Err(error) => Err(RedisError::from((
                ErrorKind::TypeError,
                "Response was of incompatible type",
                format!(
                    "Value {:?} is not parsable as instance of given struct S: {:?}",
                    v,
                    &error.to_string(),
                ),
            ))),
        }
    }

    /// Unwrap [`Value`] as an optional instance of generic struct `S`.
    pub fn to_optional_struct_instance<S: DeserializeOwned>(
        &self,
        v: &Value,
    ) -> RedisResult<Option<S>> {
        if *v == Value::Nil {
            Ok(None)
        } else {
            Ok(Some(self.to_struct_instance(v)?))
        }
    }

    /// Build new instance of [`FromRedisValueImplHandler`].
    pub fn new() -> FromRedisValueImplHandler {
        FromRedisValueImplHandler {}
    }
}
