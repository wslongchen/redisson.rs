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
use std::time::Duration;
use redis::Commands;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::{BaseDistributedObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// === RBlockingQueue(Blocking the queue) ===
pub struct RBlockingQueue<V> {
    base: BaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Serialize + DeserializeOwned> RBlockingQueue<V> {
    pub fn new(client: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: BaseDistributedObject::new(client, name),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn offer(&self, value: &V, timeout: Option<Duration>) -> RedissonResult<bool> {
        let value_json = BaseDistributedObject::serialize(value)?;
        let mut conn = self.base.get_connection()?;

        if let Some(timeout) = timeout {
            // Blocking push with timeout
            let result: Option<String> = conn.brpoplpush(self.base.get_full_key(), value_json, timeout.as_secs_f64())?;
            Ok(result.is_some())
        } else {
            // Non-blocking push
            let len: i32 = conn.rpush(self.base.get_full_key(), value_json)?;
            Ok(len > 0)
        }
    }

    pub fn poll(&self, timeout: Option<Duration>) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;

        if let Some(timeout) = timeout {
            // Blocking pop with timeout
            let result: Option<(String, String)> = conn.brpop(self.base.get_full_key(), timeout.as_secs_f64())?;
            if let Some((_, value_json)) = result {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            } else {
                Ok(None)
            }
        } else {
            // Non-blocking pop
            let result: Option<String> = conn.lpop(self.base.get_full_key(), None)?;
            match result {
                Some(value_json) => {
                    let value: V = BaseDistributedObject::deserialize(&value_json)?;
                    Ok(Some(value))
                }
                None => Ok(None),
            }
        }
    }

    pub fn peek(&self) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;
        let result: Option<String> = conn.lindex(self.base.get_full_key(), 0)?;

        match result {
            Some(value_json) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub fn size(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection()?;
        let len: i32 = conn.llen(self.base.get_full_key())?;
        Ok(len as usize)
    }

    pub fn remaining_capacity(&self) -> RedissonResult<usize> {
        // Redis lists have no capacity limit and return a large number
        Ok(usize::MAX)
    }
}