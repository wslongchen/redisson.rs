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
mod non_blocking;

pub use blocking::*;
pub use non_blocking::*;



use std::time::Instant;
use serde::Serialize;


#[derive(Clone, Debug)]
pub struct CacheEntryStats {
    pub total_entries: usize,
    pub active_entries: usize,
    pub expired_entries: usize,
    pub total_hits: u64,
    pub total_size_bytes: usize,
    pub avg_hits_per_entry: f64,
}

pub fn estimate_size<V: Serialize>(value: &V) -> usize {
    // Simple size estimation
    std::mem::size_of::<V>()
}

#[derive(Clone, Debug)]
pub struct CacheStats {
    pub total_hits: u64,
    pub total_misses: u64,
    pub total_evictions: u64,
    pub total_entries: usize,
    pub memory_usage_bytes: u64,
    pub avg_hit_rate: f64,
    pub last_cleanup: Option<Instant>,
}

impl CacheStats {
    pub fn new() -> Self {
        Self {
            total_hits: 0,
            total_misses: 0,
            total_evictions: 0,
            total_entries: 0,
            memory_usage_bytes: 0,
            avg_hit_rate: 1.0,
            last_cleanup: None,
        }
    }

    pub fn record_hit(&mut self) {
        self.total_hits += 1;
        self.update_hit_rate();
    }

    pub fn record_miss(&mut self) {
        self.total_misses += 1;
        self.update_hit_rate();
    }

    pub fn record_eviction(&mut self, count: usize) {
        self.total_evictions += count as u64;
    }

    pub fn update_hit_rate(&mut self) {
        let total = self.total_hits + self.total_misses;
        if total > 0 {
            self.avg_hit_rate = self.total_hits as f64 / total as f64;
        }
    }
}