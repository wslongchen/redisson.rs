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
mod lock;
mod fair_lock;
mod multi_lock;
mod red_lock;
mod read_write;
mod watchdog;

pub use fair_lock::*;
pub use lock::*;
pub use multi_lock::*;
pub use read_write::*;
pub use red_lock::*;
pub use watchdog::*;

use crate::{RedissonResult};
use std::time::Duration;

/// Characteristics of the lock operation of Redisson distributed objects
pub trait RLockable {
    /// Acquire a distributed lock on an object
    fn get_lock(&self) -> RLock;

    /// Acquires a fair lock on an object
    fn get_fair_lock(&self) -> RFairLock;

    /// Locking objects
    fn lock(&self) -> RedissonResult<()>;

    /// Attempt to lock the object
    fn try_lock(&self) -> RedissonResult<bool>;

    /// Attempt to lock the object with a timeout
    fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool>;

    /// Lock the object and set the lease time
    fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()>;

    /// Unlocking the object
    fn unlock(&self) -> RedissonResult<bool>;

    /// Force unlocking an object
    fn force_unlock(&self) -> RedissonResult<bool>;

    /// Check if it is locked
    fn is_locked(&self) -> RedissonResult<bool>;

    /// Checks whether the current thread holds the lock
    fn is_held_by_current_thread(&self) -> bool;
}
