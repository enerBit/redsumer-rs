#[cfg(test)]
pub mod test_from_redis_value_extended_methods {
    use redsumer::redis::Value;
    use redsumer::FromRedisValueHandler;

    use serde::{Deserialize, Serialize};

    #[test]
    fn test_numerics_from_redis_value() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        // Tests for: isize
        assert!(from_redis_value_handler
            .to_isize(&(Value::Data(String::from("-255").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_isize(&(Value::Data(String::from("a").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_isize(&(Value::Data(String::from("255").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_isize(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: i8
        assert!(from_redis_value_handler
            .to_i8(&(Value::Data(String::from("127").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_i8(&(Value::Data(String::from("-130").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_i8(&(Value::Data(String::from("127").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_i8(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: i16
        assert!(from_redis_value_handler
            .to_i16(&(Value::Data(String::from("127").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_i16(&(Value::Data(String::from("a").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_i16(&(Value::Data(String::from("127").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_i16(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: i32
        assert!(from_redis_value_handler
            .to_i32(&(Value::Data(String::from("127").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_i32(&(Value::Data(String::from("a").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_i32(&(Value::Data(String::from("127").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_i32(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: i64
        assert!(from_redis_value_handler
            .to_i64(&(Value::Data(String::from("127").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_i64(&(Value::Data(String::from("a").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_i64(&(Value::Data(String::from("127").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_i64(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: i128
        assert!(from_redis_value_handler
            .to_i128(&(Value::Data(String::from("127").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_i128(&(Value::Data(String::from("a").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_i128(&(Value::Data(String::from("127").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_i128(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: usize
        assert!(from_redis_value_handler
            .to_usize(&(Value::Data(String::from("255").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_usize(&(Value::Data(String::from("-1").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_usize(&(Value::Data(String::from("255").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_usize(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: u8
        assert!(from_redis_value_handler
            .to_u8(&(Value::Data(String::from("255").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_u8(&(Value::Data(String::from("-1").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_u8(&(Value::Data(String::from("255").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_u8(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for u16:
        assert!(from_redis_value_handler
            .to_u16(&(Value::Data(String::from("255").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_u16(&(Value::Data(String::from("-1").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_u16(&(Value::Data(String::from("65535").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_u16(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for u32:
        assert!(from_redis_value_handler
            .to_u32(&(Value::Data(String::from("255").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_u32(&(Value::Data(String::from("-1").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_u32(&(Value::Data(String::from("255").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_u32(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for u64:
        assert!(from_redis_value_handler
            .to_u64(&(Value::Data(String::from("255").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_u64(&(Value::Data(String::from("-1").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_u64(&(Value::Data(String::from("255").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_u64(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: u128
        assert!(from_redis_value_handler
            .to_u128(&(Value::Data(String::from("127").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_u128(&(Value::Data(String::from("a").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_u128(&(Value::Data(String::from("127").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_u128(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: f32
        assert!(from_redis_value_handler
            .to_f32(&(Value::Data(String::from("1345.5678").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_f32(&(Value::Data(String::from("a").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_f32(&(Value::Data(String::from("1345.5678").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_f32(&(Value::Nil))
            .unwrap()
            .is_none());

        // Tests for: f64
        assert!(from_redis_value_handler
            .to_f64(&(Value::Data(String::from("1345.5678").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_f64(&(Value::Data(String::from("a").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_f64(&(Value::Data(String::from("1345.5678").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_f64(&(Value::Nil))
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_get_string_from_redis_value() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        assert!(from_redis_value_handler
            .to_optional_string(&(Value::Data(String::from("hello-rusty").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_string(&(Value::Nil))
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_bool_from_redis_value() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        assert!(from_redis_value_handler
            .to_bool(&(Value::Data(String::from("1").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_bool(&(Value::Data(String::from("0").into_bytes())))
            .is_ok());

        assert!(from_redis_value_handler
            .to_bool(&(Value::Data(String::from("-3").into_bytes())))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_bool(&(Value::Data(String::from("1").into_bytes())))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_bool(&(Value::Nil))
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_uuid_from_redis_value() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        assert!(from_redis_value_handler
            .to_uuid(
                &(Value::Data(String::from("2983cfeb-e2e0-4f21-b33e-bf678cb67f79").into_bytes()))
            )
            .is_ok());

        assert!(from_redis_value_handler
            .to_uuid(
                &(Value::Data(String::from("983cfeb-e2e0-4f21-b33e-bf678cb67f79").into_bytes()))
            )
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_uuid(
                &(&(Value::Data(
                    String::from("2983cfeb-e2e0-4f21-b33e-bf678cb67f79").into_bytes()
                )))
            )
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_uuid(&Value::Nil)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_get_offsetdatetime_from_redis_value_in_format_iso8601() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        assert!(from_redis_value_handler
            .to_offsetdatetime_from_iso8601(
                &(Value::Data(String::from("2024-01-15T21:19:00+0800").into_bytes()))
            )
            .is_ok());

        assert!(from_redis_value_handler
            .to_offsetdatetime_from_iso8601(
                &(Value::Data(String::from("2024-01-15 21:19:00.000-05:00").into_bytes()))
            )
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_offsetdatetime_from_iso8601(
                &(Value::Data(String::from("2024-01-15T21:19:00+0800").into_bytes()))
            )
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_offsetdatetime_from_iso8601(&Value::Nil)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_get_offsetdatetime_from_redis_value_in_format_rfc2822() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        assert!(from_redis_value_handler
            .to_offsetdatetime_from_rfc2822(
                &(Value::Data(String::from("Fri, 15 Jan 2024 21:19:00 -0500").into_bytes()))
            )
            .is_ok());

        assert!(from_redis_value_handler
            .to_offsetdatetime_from_rfc2822(
                &(Value::Data(String::from("2024-01-15 21:19:00.000-05:00").into_bytes()))
            )
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_offsetdatetime_from_rfc2822(
                &(Value::Data(String::from("Fri, 15 Jan 2024 21:19:00 -0500").into_bytes()))
            )
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_offsetdatetime_from_rfc2822(&Value::Nil)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_get_offsetdatetime_from_redis_value_in_format_rfc3339() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        assert!(from_redis_value_handler
            .to_offsetdatetime_from_rfc3339(&Value::Data(
                String::from("2024-01-15T21:19:00.000-05:00").into_bytes()
            ))
            .is_ok());

        assert!(from_redis_value_handler
            .to_offsetdatetime_from_rfc3339(&Value::Data(
                String::from("2024-01-15 21:19:00.000-05:00").into_bytes()
            ))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_offsetdatetime_from_rfc3339(&Value::Data(
                String::from("2024-01-15T21:19:00.000-05:00").into_bytes()
            ))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_offsetdatetime_from_rfc3339(&Value::Nil)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_get_date_from_redis_value_in_format_iso8601() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        assert!(from_redis_value_handler
            .to_date_from_iso8601(&Value::Data(String::from("2024-01-16").into_bytes()))
            .is_ok());

        assert!(from_redis_value_handler
            .to_date_from_iso8601(&Value::Data(String::from("16-01-2024").into_bytes()))
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_date_from_iso8601(&Value::Data(String::from("2024-01-16").into_bytes()))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_date_from_iso8601(&Value::Nil)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_get_bytes_from_redis_value() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        assert!(from_redis_value_handler
            .to_bytes(&Value::Data(
                String::from("name,last_name\r\nJuan,Manuel").into_bytes()
            ))
            .is_ok());

        assert!(from_redis_value_handler
            .to_optional_bytes(&Value::Data(String::from("true").into_bytes()))
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_bytes(&Value::Nil)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_get_struct_instance_from_redis_value() {
        let from_redis_value_handler: FromRedisValueHandler = FromRedisValueHandler::new();

        #[derive(Deserialize, Serialize)]
        struct Person {
            pub name: String,
            pub last_name: String,
            pub age: u8,
            pub localization: Localization,
            pub is_live: bool,
        }

        #[derive(Deserialize, Serialize)]
        struct Localization {
            pub city: String,
            pub state: String,
            pub country: String,
        }

        let json_as_value: Value = Value::Data(
            String::from(
                r#"
                    {
                        "name":"Juan",
                        "last_name":"Tamayo",
                        "age":30,
                        "is_live": false,
                        "localization": {
                            "country":"COL",
                            "state":"ANT",
                            "city":"MDE"
                        },
                        "favorite_food":"frijolitos",
                        "best_friend":"Miken't"
                    }"#,
            )
            .into_bytes(),
        );

        let incorrect_json_as_value: Value = Value::Data(
            String::from(r#"{"name":"Juan","middle_name":"Manuel","age":30}"#).into_bytes(),
        );

        assert!(from_redis_value_handler
            .to_struct_instance::<Person>(&json_as_value)
            .is_ok());

        assert!(from_redis_value_handler
            .to_struct_instance::<Person>(&incorrect_json_as_value)
            .is_err());

        assert!(from_redis_value_handler
            .to_optional_struct_instance::<Person>(&json_as_value)
            .unwrap()
            .is_some());

        assert!(from_redis_value_handler
            .to_optional_struct_instance::<Person>(&Value::Nil)
            .unwrap()
            .is_none());
    }
}
