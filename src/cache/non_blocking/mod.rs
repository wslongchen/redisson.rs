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
mod local;
mod redis;

use async_trait::async_trait;
pub use local::*;
pub use redis::*;
use crate::RedissonResult;

#[async_trait]
pub trait AsyncCache<K, V> {
    async fn get(&self, key: &K) -> RedissonResult<Option<V>>;
    async fn set(&self, key: K, value: V) -> RedissonResult<()>;
    async fn remove(&self, key: &K) -> RedissonResult<bool>;
    async fn clear(&self) -> RedissonResult<()>;
    async fn refresh(&self, key: &K) -> RedissonResult<bool>;
}