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

use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;
use crate::{thread_id_to_u64};

/// 本地锁状态
struct LocalLockState {
    lock_count: u32,
    lock_value: String,
    last_renew_time: Instant,
}




/// === 基础锁信息 ===
#[derive(Debug, Clone)]
pub struct LockInfo {
    pub name: String,
    pub value: String,
    pub thread_id: u64,
    pub lease_time: Duration,
    pub acquired_at: SystemTime,
    pub expire_time: SystemTime,
}


impl LockInfo {
    pub fn new(name: String, lease_time: Duration) -> Self {
        let value = Uuid::new_v4().to_string();
        let thread_id = thread_id_to_u64();
        let acquired_at = SystemTime::now();
        let expire_time = acquired_at + lease_time;

        Self {
            name,
            value,
            thread_id,
            lease_time,
            acquired_at,
            expire_time,
        }
    }

    pub fn is_expired(&self) -> bool {
        SystemTime::now() >= self.expire_time
    }

    pub fn remaining_time(&self) -> Duration {
        self.expire_time
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::from_secs(0))
    }
}

/// 红锁本地状态
struct RedLockLocalState {
    lock_value: Option<String>,
    acquired_at: Option<Instant>,
    acquired_nodes: Vec<usize>, // 成功获取锁的节点索引
}

