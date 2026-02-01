/*
 *
 *  *
 *  *      Copyright (c) 2018-2025, SnackCloud All rights reserved.
 *  *
 *  *   Redistribution and use in source and binary forms, with or without
 *  *   modification, are permitted provided that the following conditions are met:
 *  *
 *  *   Redistributions of source code must retain the above copyright notice,
 *  *   this list of conditions and the following disclaimer.
 *  *   Redistributions in binary form must reproduce the above copyright
 *  *   notice, this list of conditions and the following disclaimer in the
 *  *   documentation and/or other materials provided with the distribution.
 *  *   Neither the name of the www.snackcloud.cn developer nor the names of its
 *  *   contributors may be used to endorse or promote products derived from
 *  *   this software without specific prior written permission.
 *  *   Author: SnackCloud
 *  *
 *  
 */

mod blocking;
#[cfg(feature = "async")]
mod non_blocking;


pub use blocking::*;
#[cfg(feature = "async")]
pub use non_blocking::*;


use std::collections::HashMap;
use redis::{geo, FromRedisValue, ParsingError};
use redis::geo::{RadiusOptions, Unit};
use crate::RedisMapExt;

#[derive(Debug, Clone)]
pub enum ListPosition {
    Before,
    After,
}

#[derive(Debug, Clone)]
pub enum ListEnd {
    Left,
    Right,
}


#[derive(Debug, Clone)]
pub enum GeoUnit {
    Meters,
    Kilometers,
    Miles,
    Feet,
}

impl GeoUnit {
    pub fn as_str(&self) -> &str {
        match self {
            GeoUnit::Meters => "m",
            GeoUnit::Kilometers => "km",
            GeoUnit::Miles => "mi",
            GeoUnit::Feet => "ft",
        }
    }
}


impl From<GeoUnit> for Unit {
    fn from(value: GeoUnit) -> Unit {
        match value {
            GeoUnit::Meters => Unit::Meters,
            GeoUnit::Kilometers => Unit::Kilometers,
            GeoUnit::Miles => Unit::Miles,
            GeoUnit::Feet => Unit::Feet,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum GeoSort {
    Asc,
    Desc,
}


/// Redis Stream MESSAGE
#[derive(Debug, Clone)]
pub struct StreamMessage<V> {
    pub id: String,
    pub fields: HashMap<String, V>,
}

/// Stream Consumption Section
#[derive(Debug, Clone)]
pub struct StreamGroup {
    pub name: String,
    pub consumers: Vec<String>,
    pub pending_count: u64,
    pub last_delivered_id: String,
}

/// Stream 消费者
#[derive(Debug, Clone)]
pub struct StreamConsumer {
    pub name: String,
    pub pending_count: u64,
    pub idle_time_ms: u64,
}

/// Stream 信息
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub length: u64,
    pub radix_tree_keys: u64,
    pub radix_tree_nodes: u64,
    pub groups: u64,
    pub last_generated_id: String,
    pub first_entry: Option<StreamMessage<String>>,
    pub last_entry: Option<StreamMessage<String>>,
}

impl FromRedisValue for StreamInfo {
    fn from_redis_value(v: redis::Value) -> Result<StreamInfo, ParsingError> {
        let map: HashMap<String, redis::Value> = HashMap::from_redis_value(v)?;

        // 解析消息条目
        fn parse_message_entry(value: &redis::Value) -> Option<StreamMessage<String>> {
            if let redis::Value::Array(items) = value {
                if items.len() >= 2 {
                    // 解析 ID
                    let id = match &items[0] {
                        redis::Value::BulkString(data) =>
                            String::from_utf8(data.clone()).ok()?,
                        redis::Value::SimpleString(s) => s.clone(),
                        _ => return None,
                    };

                    // 解析字段
                    let mut fields = HashMap::new();
                    if let redis::Value::Array(field_items) = &items[1] {
                        for i in (0..field_items.len()).step_by(2) {
                            if i + 1 < field_items.len() {
                                let field = match &field_items[i] {
                                    redis::Value::BulkString(data) =>
                                        String::from_utf8(data.clone()).ok()?,
                                    redis::Value::SimpleString(s) => s.clone(),
                                    _ => continue,
                                };

                                let value = match &field_items[i + 1] {
                                    redis::Value::BulkString(data) =>
                                        String::from_utf8(data.clone()).ok()?,
                                    redis::Value::SimpleString(s) => s.clone(),
                                    redis::Value::Int(num) => num.to_string(),
                                    redis::Value::Double(num) => num.to_string(),
                                    _ => continue,
                                };

                                fields.insert(field, value);
                            }
                        }
                    }

                    return Some(StreamMessage { id, fields });
                }
            }
            None
        }

        let first_entry = map.get("first-entry")
            .and_then(|v| parse_message_entry(v));

        let last_entry = map.get("last-entry")
            .and_then(|v| parse_message_entry(v));

        Ok(StreamInfo {
            length: map.get_i64("length") as u64,
            radix_tree_keys: map.get_i64("radix-tree-keys") as u64,
            radix_tree_nodes: map.get_i64("radix-tree-nodes") as u64,
            groups: map.get_i64("groups") as u64,
            last_generated_id: map.get_string("last-generated-id"),
            first_entry,
            last_entry,
        })
    }
}



#[derive(Debug, Clone)]
pub struct PendingMessage {
    pub id: String,
    pub consumer: String,
    pub idle_ms: u64,
    pub delivery_count: u32,
}


// Bloom Filter information structure
#[derive(Debug, Clone, Default)]
pub struct BloomFilterInfo {
    pub capacity: usize,
    pub size: usize,
    pub number_of_filters: usize,
    pub items_inserted: usize,
    pub expansion_rate: usize,
}


#[derive(Debug, Clone)]
pub struct GeoRadiusOptions {
    pub with_coord: bool,
    pub with_dist: bool,
    pub with_hash: bool,
    pub count: Option<i64>,
    pub sort: GeoSort,
    pub store: Option<String>,
    pub store_dist: Option<String>,
}

impl From<GeoRadiusOptions> for geo::RadiusOptions {
    fn from(value: GeoRadiusOptions) -> Self {
        let mut v = RadiusOptions::default();
        if value.with_coord {
            v = v.with_coord();
        }
        if value.with_dist {
            v = v.with_dist();
        }
        if let Some(count) = value.count {
            v = v.limit(count as usize);
        }
        if let Some(store) = value.store {
            v = v.store(store);
        }
        if let Some(store_dist) = value.store_dist {
            v = v.store_dist(store_dist);
        }
        v
    }
}

impl Default for GeoRadiusOptions {
    fn default() -> Self {
        Self {
            with_coord: false,
            with_dist: false,
            with_hash: false,
            count: None,
            sort: GeoSort::Asc,
            store: None,
            store_dist: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct GeoRadiusResult<V> {
    pub member: Option<V>,
    pub distance: Option<f64>,
    pub hash: Option<u64>,
    pub coordinate: Option<(f64, f64)>,
}
