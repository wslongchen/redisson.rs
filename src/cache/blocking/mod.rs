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

pub use local::*;
pub use redis::*;

use std::time::Duration;
use async_trait::async_trait;

use crate::RedissonResult;

#[async_trait]
pub trait Cache<K, V> {
    fn get(&self, key: &K) -> RedissonResult<Option<V>>;
    fn set(&self, key: K, value: V) -> RedissonResult<()>;

    fn set_with_ttl(&self, key: K, value: V, ttl: Duration) -> RedissonResult<()>;
    fn remove(&self, key: &K) -> RedissonResult<bool>;
    fn clear(&self) -> RedissonResult<()>;
    fn refresh(&self, key: &K) -> RedissonResult<bool>;
}

