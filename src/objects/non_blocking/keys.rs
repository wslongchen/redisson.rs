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
use std::sync::Arc;
use redis::AsyncCommands;
use crate::{AsyncRedisConnectionManager, RedissonResult};

/// Key manipulation tool
pub struct AsyncRKeys {
    connection_manager: Arc<AsyncRedisConnectionManager>,
}

impl AsyncRKeys {
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>) -> Self {
        Self { connection_manager }
    }

    pub async fn delete(&self, pattern: &str) -> RedissonResult<u64> {
        let mut conn = self.connection_manager.get_connection().await?;
        let keys: Vec<String> = conn.keys(pattern).await?;

        if keys.is_empty() {
            return Ok(0);
        }

        let deleted: i32 = conn.del(keys).await?;
        Ok(deleted as u64)
    }

    pub async fn find_keys_by_pattern(&self, pattern: &str) -> RedissonResult<Vec<String>> {
        let mut conn = self.connection_manager.get_connection().await?;
        let keys: Vec<String> = conn.keys(pattern).await?;
        Ok(keys)
    }

    pub async fn flush_all(&self) -> RedissonResult<()> {
        let mut conn = self.connection_manager.get_connection().await?;
        redis::cmd("FLUSHALL").query_async::<()>(&mut conn).await?;
        Ok(())
    }

    pub async fn flush_db(&self) -> RedissonResult<()> {
        let mut conn = self.connection_manager.get_connection().await?;
        redis::cmd("FLUSHDB").query_async::<()>(&mut conn).await?;
        Ok(())
    }
}
