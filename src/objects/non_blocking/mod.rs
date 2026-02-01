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
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
pub use keys::*;
pub use base::*;
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
use crate::{AsyncRedisConnection, RedissonResult};

/// Asynchronous version of a distributed object
#[async_trait]
pub trait AsyncRObject {
    async fn delete(&self) -> RedissonResult<bool>;
    async fn rename(&self, new_name: &str) -> RedissonResult<()>;
    async fn is_exists(&self) -> RedissonResult<bool>;
    async fn move_to_db(&self, db_index: i32) -> RedissonResult<bool>;
    async fn get_expire_time(&self) -> RedissonResult<Option<Duration>>;
    async fn expire(&self, duration: Duration) -> RedissonResult<bool>;
    async fn expire_at(&self, timestamp: i64) -> RedissonResult<bool>;
    async fn clear_expire(&self) -> RedissonResult<bool>;
}


#[async_trait]
pub trait AsyncRObjectBase<V>: AsyncRObject
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn get_connection(&self) -> RedissonResult<AsyncRedisConnection>;

    fn serialize_value(&self, value: &V) -> RedissonResult<String>;
    fn deserialize_value(&self, data: &str) -> RedissonResult<V>;
    fn get_full_key(&self) -> String;

    // 通用异步方法
    async fn get(&self) -> RedissonResult<Option<V>> {
        let mut conn = self.get_connection().await?;
        let result: Option<String> = conn
            .execute_command(&mut redis::cmd("GET").arg(self.get_full_key()))
            .await?;

        result.map(|data| self.deserialize_value(&data)).transpose()
    }

    async fn set(&self, value: &V) -> RedissonResult<()> {
        let mut conn = self.get_connection().await?;
        let serialized = self.serialize_value(value)?;

        conn.execute_command::<()>(&mut redis::cmd("SET").arg(self.get_full_key()).arg(&serialized))
            .await?;
        Ok(())
    }

    async fn set_with_ttl(&self, value: &V, ttl: Duration) -> RedissonResult<()> {
        let mut conn = self.get_connection().await?;
        let serialized = self.serialize_value(value)?;
        let seconds = ttl.as_secs() as i64;

        conn.execute_command::<()>(
            &mut redis::cmd("SETEX")
                .arg(self.get_full_key())
                .arg(seconds)
                .arg(&serialized)
        ).await?;
        Ok(())
    }
}