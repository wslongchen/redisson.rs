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

use crate::LatencyStats;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

/// Asynchronous network latency statistics
pub struct AsyncNetworkLatencyStats {
    samples: TokioMutex<VecDeque<Duration>>,
    max_samples: usize,
    current_estimate: Arc<AtomicU64>,
}

impl AsyncNetworkLatencyStats {
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: TokioMutex::new(VecDeque::with_capacity(max_samples)),
            max_samples,
            current_estimate: Arc::new(AtomicU64::new(10_000_000)),
        }
    }

    pub async fn add_sample(&self, latency: Duration) {
        let mut samples = self.samples.lock().await;

        samples.push_back(latency);

        if samples.len() > self.max_samples {
            samples.pop_front();
        }

        if samples.len() >= 3 {
            let mut sorted: Vec<Duration> = samples.iter().copied().collect();
            sorted.sort();

            let index = (sorted.len() as f64 * 0.95).floor() as usize;
            let p95_latency = sorted[index.min(sorted.len() - 1)];

            let estimate = p95_latency * 3 / 2;
            self.current_estimate.store(estimate.as_nanos() as u64, Ordering::Release);
        }
    }

    pub fn get_estimate(&self) -> Duration {
        Duration::from_nanos(self.current_estimate.load(Ordering::Acquire))
    }

    pub async fn get_stats(&self) -> LatencyStats {
        let samples = self.samples.lock().await;
        let mut sorted: Vec<Duration> = samples.iter().copied().collect();
        sorted.sort();

        let count = sorted.len();
        if count == 0 {
            return LatencyStats {
                min: Duration::from_millis(0),
                max: Duration::from_millis(0),
                avg: Duration::from_millis(0),
                p95: Duration::from_millis(0),
                p99: Duration::from_millis(0),
                count: 0,
            };
        }

        let min = sorted[0];
        let max = sorted[count - 1];
        let sum: Duration = sorted.iter().sum();
        let avg = sum / count as u32;

        let p95_index = (count as f64 * 0.95).floor() as usize;
        let p95 = sorted[p95_index.min(count - 1)];

        let p99_index = (count as f64 * 0.99).floor() as usize;
        let p99 = sorted[p99_index.min(count - 1)];

        LatencyStats {
            min,
            max,
            avg,
            p95,
            p99,
            count,
        }
    }
}
