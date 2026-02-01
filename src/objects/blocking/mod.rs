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
mod base;
mod stream;
mod distributed;
mod bucket;
mod map;
mod list;
mod set;
mod sort_set;
mod bloom_filter;
mod delayed_queue;
mod script;
mod batch;
mod atomic_long;
mod semaphore;
mod rate_limiter;
mod count_down_latch;
mod bit_set;
mod geo;
mod topic;
mod blocking_queue;
mod keys;

use std::time::Duration;
use serde::de::DeserializeOwned;
use serde::Serialize;
pub use base::*;
pub use distributed::*;
pub use keys::*;
pub use stream::*;
pub use bucket::*;
pub use map::*;
pub use list::*;
pub use set::*;
pub use sort_set::*;
pub use bloom_filter::*;
pub use delayed_queue::*;
pub use script::*;
pub use batch::*;
pub use atomic_long::*;
pub use semaphore::*;
pub use rate_limiter::*;
pub use count_down_latch::*;
pub use bit_set::*;
pub use geo::*;
pub use topic::*;
pub use blocking_queue::*;
use crate::{RedisConnection, RedissonResult};

/// The base trait of all distributed objects
pub trait RObject {
    /// Getting the object name
    fn get_name(&self) -> &str;

    /// Deleting objects
    fn delete(&self) -> RedissonResult<bool>;

    /// Renaming Objects
    fn rename(&self, new_name: &str) -> RedissonResult<()>;

    /// Checks if the object exists
    fn is_exists(&self) -> RedissonResult<bool>;

    /// Move objects to other databases
    fn move_to_db(&self, db_index: i32) -> RedissonResult<bool>;

    /// Getting the expiration time
    fn get_expire_time(&self) -> RedissonResult<Option<Duration>>;

    /// Set an expiration time
    fn expire(&self, duration: Duration) -> RedissonResult<bool>;

    /// Set an expiration point
    fn expire_at(&self, timestamp: i64) -> RedissonResult<bool>;

    /// Clear the expiration time
    fn clear_expire(&self) -> RedissonResult<bool>;
}

/// The base trait of an encodable object
pub trait RObjectBase<V>: RObject
where
    V: Serialize + DeserializeOwned + Send + Sync,
{
    /// Getting connections
    fn get_connection(&self) -> RedissonResult<RedisConnection>;

    /// Serializing values
    fn serialize_value(&self, value: &V) -> RedissonResult<String>;

    /// Deserialize values
    fn deserialize_value(&self, data: &str) -> RedissonResult<V>;

    /// Gets the full key (overridden by subclasses to add a prefix)
    fn get_full_key(&self) -> String {
        self.get_name().to_string()
    }
}