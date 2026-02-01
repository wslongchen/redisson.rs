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


use std::time::Duration;

/// 延迟统计信息
#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub min: Duration,
    pub max: Duration,
    pub avg: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub count: usize,
}

impl std::fmt::Display for LatencyStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "count={}, min={:.2?}, max={:.2?}, avg={:.2?}, p95={:.2?}, p99={:.2?}",
               self.count, self.min, self.max, self.avg, self.p95, self.p99)
    }
}
