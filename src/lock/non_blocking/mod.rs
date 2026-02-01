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
use std::time::Duration;
use async_trait::async_trait;
use crate::RedissonResult;

mod lock;
mod fair_lock;
mod multi_lock;
mod red_lock;
mod watchdog;
mod read_write;

pub use read_write::*;
pub use lock::*;
pub use fair_lock::*;
pub use multi_lock::*;
pub use red_lock::*;

/// Asynchronous version of the locking operation
#[async_trait]
pub trait AsyncRLockable {
    fn get_lock(&self) -> AsyncRLock;
    fn get_fair_lock(&self) -> AsyncRFairLock;

    async fn lock(&self) -> RedissonResult<()>;
    async fn try_lock(&self) -> RedissonResult<bool>;
    async fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool>;
    async fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()>;
    async fn unlock(&self) -> RedissonResult<bool>;
    async fn force_unlock(&self) -> RedissonResult<bool>;
    async fn is_locked(&self) -> RedissonResult<bool>;
    async fn is_held_by_current_thread(&self) -> bool;
}