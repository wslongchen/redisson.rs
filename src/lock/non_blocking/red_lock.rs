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
use std::time::{Duration, Instant};
use redis::AsyncTypedCommands;
use tokio::sync::{Mutex as TokioMutex, MutexGuard};
use tokio::time::{sleep, timeout};
use tracing::{debug, info};
use uuid::Uuid;
use crate::{scripts, AsyncRedisConnectionManager, AsyncNetworkLatencyStats, RedissonError, RedissonResult};
use crate::lock::RedLockLocalState;

/// === AsyncRRedLock (Asynchronous red lock) ===
pub struct AsyncRRedLock {
    connection_managers: Vec<Arc<AsyncRedisConnectionManager>>,
    name: String,
    lease_time: Duration,
    drift_factor: f64,
    local_state: Arc<TokioMutex<RedLockLocalState>>,
    latency_stats: Arc<AsyncNetworkLatencyStats>,
}

impl AsyncRRedLock {
    pub fn new(
        connection_managers: Vec<Arc<AsyncRedisConnectionManager>>,
        name: String,
        lease_time: Duration,
    ) -> Self {
        Self {
            connection_managers,
            name,
            lease_time,
            drift_factor: 0.01,
            local_state: Arc::new(TokioMutex::new(RedLockLocalState {
                lock_value: None,
                acquired_at: None,
                acquired_nodes: Vec::new(),
            })),
            latency_stats: Arc::new(AsyncNetworkLatencyStats::new(100)),
        }
    }

    pub fn with_drift_factor(mut self, drift_factor: f64) -> Self {
        self.drift_factor = drift_factor;
        self
    }

    /// Acquire locks asynchronously
    pub async fn lock(&self) -> RedissonResult<()> {
        let stats = self.latency_stats.get_stats().await;
        if stats.count < 3 {
            debug!("Automatically warm up network latency measurements...");
            self.warmup_latency_measurement(5).await;
        }
        self.lock_with_retries(3, Duration::from_millis(200)).await
    }

    /// Asynchronous lock acquisition with retry
    pub async fn lock_with_retries(&self, max_retries: u32, retry_delay: Duration) -> RedissonResult<()> {
        for attempt in 0..max_retries {
            match self.try_lock_once().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt == max_retries - 1 {
                        return Err(e);
                    }
                    sleep(retry_delay).await;
                }
            }
        }
        Err(RedissonError::LockAcquisitionError)
    }

    /// Single-attempt lock acquisition (asynchronous)
    async fn try_lock_once(&self) -> RedissonResult<()> {
        // Check if the lock is already held
        {
            let state = self.local_state.lock().await;
            if state.lock_value.is_some() {
                return Ok(());
            }
        }

        let quorum = self.calculate_quorum();
        let lock_value = Uuid::new_v4().to_string();
        let mut successes = 0;
        let mut acquired_nodes = Vec::new();
        let start_time = Instant::now();

        // Attempt to acquire locks at all nodes in parallel
        let mut tasks = Vec::new();
        for (i, manager) in self.connection_managers.iter().enumerate() {
            let manager = manager.clone();
            let name = self.name.clone();
            let lock_value = lock_value.clone();
            let lease_time = self.lease_time;

            tasks.push(tokio::spawn(async move {
                match manager.get_connection().await {
                    Ok(mut conn) => {
                        let result: Result<i32, _> = scripts::LOCK_SCRIPT
                            .key(&name)
                            .arg(&lock_value)
                            .arg(lease_time.as_millis() as i64)
                            .invoke_async(&mut conn)
                            .await;

                        match result {
                            Ok(acquired) if acquired > 0 => Some((i, true)),
                            _ => Some((i, false)),
                        }
                    }
                    Err(_) => Some((i, false)),
                }
            }));
        }

        // Collecting results
        for task in tasks {
            if let Ok(Some((i, success))) = task.await {
                if success {
                    successes += 1;
                    acquired_nodes.push(i);
                }
            }
        }

        // Computing valid time
        let total_elapsed = start_time.elapsed();
        
        // A modified valid time calculation is used
        let validity_time = self.calculate_validity_time(total_elapsed, successes).await;

        // Determining success
        if successes >= quorum && validity_time.as_millis() > 0 {
            // Saving state
            let mut state = self.local_state.lock().await;
            state.lock_value = Some(lock_value.clone());
            state.acquired_at = Some(start_time);
            state.acquired_nodes = acquired_nodes;

            // Start an asynchronous renewal task
            if self.lease_time.as_secs() > 0 {
                self.start_async_renewal_task(lock_value, validity_time).await;
            }

            Ok(())
        } else {
            // Clean up
            self.cleanup_partial_locks_async(&lock_value, &acquired_nodes).await;

            if successes < quorum {
                Err(RedissonError::LockAcquisitionError)
            } else {
                Err(RedissonError::TimeoutError)
            }
        }
    }

    /// Release locks asynchronously
    pub async fn unlock(&self) -> RedissonResult<bool> {
        let (lock_value, acquired_nodes) = {
            let mut state = self.local_state.lock().await;
            let lock_value = state.lock_value.take();
            let acquired_nodes = std::mem::take(&mut state.acquired_nodes);
            state.acquired_at = None;
            (lock_value, acquired_nodes)
        };

        if let Some(lock_value) = lock_value {
            let mut successes = 0;

            // Releasing locks in parallel
            let mut tasks = Vec::new();
            for &node_idx in &acquired_nodes {
                if node_idx < self.connection_managers.len() {
                    let manager = self.connection_managers[node_idx].clone();
                    let name = self.name.clone();
                    let lock_value = lock_value.clone();

                    tasks.push(tokio::spawn(async move {
                        match manager.get_connection().await {
                            Ok(mut conn) => {
                                let result: Result<i32, _> = scripts::UNLOCK_SCRIPT
                                    .key(&name)
                                    .arg(&lock_value)
                                    .invoke_async(&mut conn)
                                    .await;

                                result.unwrap_or(0) > 0
                            }
                            Err(_) => false,
                        }
                    }));
                }
            }

            // Collecting results
            for task in tasks {
                if let Ok(success) = task.await {
                    if success {
                        successes += 1;
                    }
                }
            }

            // A majority of nodes are required to be released successfully
            let min_releases = (acquired_nodes.len() / 2) + 1;
            Ok(successes >= min_releases)
        } else {
            Ok(false)
        }
    }

    /// An asynchronous attempt to acquire the lock
    pub async fn try_lock(&self) -> RedissonResult<bool> {
        match self.try_lock_once().await {
            Ok(()) => Ok(true),
            Err(RedissonError::LockAcquisitionError) => Ok(false),
            Err(RedissonError::TimeoutError) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Asynchronously clean up partially successful locks
    async fn cleanup_partial_locks_async(&self, lock_value: &str, acquired_nodes: &[usize]) {
        let mut tasks = Vec::new();

        for &node_idx in acquired_nodes {
            if node_idx < self.connection_managers.len() {
                let manager = self.connection_managers[node_idx].clone();
                let name = self.name.clone();
                let lock_value = lock_value.to_string();

                tasks.push(tokio::spawn(async move {
                    if let Ok(mut conn) = manager.get_connection().await {
                        let _ = scripts::UNLOCK_SCRIPT
                            .key(&name)
                            .arg(&lock_value)
                            .invoke_async::<i32>(&mut conn)
                            .await;
                    }
                }));
            }
        }

        for task in tasks {
            let _ = task.await;
        }
    }

    /// Start an asynchronous renewal task
    async fn start_async_renewal_task(&self, lock_value: String, initial_validity: Duration) {
        let connection_managers = self.connection_managers.clone();
        let name = self.name.clone();
        let lease_time = self.lease_time;
        let renew_interval = initial_validity / 3;

        tokio::spawn(async move {
            let mut is_running = true;

            while is_running {
                sleep(renew_interval).await;

                // Parallel renewal
                let mut tasks = Vec::new();
                for manager in &connection_managers {
                    let manager = manager.clone();
                    let name = name.clone();
                    let lock_value = lock_value.clone();
                    let lease_time = lease_time;

                    tasks.push(tokio::spawn(async move {
                        match manager.get_connection().await {
                            Ok(mut conn) => {
                                let result: Result<i32, _> = scripts::RENEW_SCRIPT
                                    .key(&name)
                                    .arg(&lock_value)
                                    .arg(lease_time.as_millis() as i64)
                                    .invoke_async(&mut conn)
                                    .await;

                                result.unwrap_or(0) > 0
                            }
                            Err(_) => false,
                        }
                    }));
                }

                // Collecting results
                let mut successes = 0;
                for task in tasks {
                    if let Ok(success) = task.await {
                        if success {
                            successes += 1;
                        }
                    }
                }

                // Check if the renewal was successful
                let quorum = (connection_managers.len() / 2) + 1;
                if successes < quorum {
                    is_running = false;
                }
            }
        });
    }

    /// Calculation of legal quantity
    fn calculate_quorum(&self) -> usize {
        let n = self.connection_managers.len();
        n / 2 + 1
    }

    /// Get the number of healthy nodes
    pub async fn healthy_node_count(&self) -> usize {
        let mut healthy_count = 0;
        for connector in self.connection_managers
            .iter() {
            if connector.health_check().await {
                healthy_count += 1;
            }
        }
        healthy_count
    }
    
    /// Calculate more accurate valid times
    async fn calculate_validity_time(&self, elapsed: Duration, acquired_nodes: usize) -> Duration {
        let quorum = self.calculate_quorum();

        if acquired_nodes < quorum {
            return Duration::from_secs(0);
        }

        // Basic effective time
        let basic_validity = if elapsed < self.lease_time {
            self.lease_time - elapsed
        } else {
            Duration::from_secs(0)
        };

        if basic_validity == Duration::from_secs(0) {
            return basic_validity;
        }

        // The drift factor is calculated dynamically
        let drift_factor = self.calculate_dynamic_drift_factor().await;
        let drift = Duration::from_millis(
            (drift_factor * self.lease_time.as_millis() as f64) as u64
        );

        // Network delay bound
        let network_margin = self.estimate_network_margin().await;

        // Node health compensation
        let healthy_nodes = self.healthy_node_count().await;
        let node_health_penalty = if healthy_nodes < self.connection_managers.len() {
            // There are nodes that are not healthy, increasing the safety margin
            Duration::from_millis(5)
        } else {
            Duration::from_millis(0)
        };

        // The final valid time
        let total_margin = drift + network_margin + node_health_penalty + Duration::from_millis(2);

        basic_validity.checked_sub(total_margin).unwrap_or(Duration::from_secs(0))
    }


    /// Measuring Network Round-trip Time (Ping)
    async fn measure_network_rtt(&self) -> Duration {
        let mut total_rtt = Duration::from_secs(0);
        let mut successful_measurements = 0;

        for manager in &self.connection_managers {
            let start = Instant::now();

            // Execute a simple Redis command to measure the RTT
            if let Ok(mut conn) = manager.get_connection().await {
                if conn.ping().await.is_ok() {
                    let rtt = start.elapsed();
                    total_rtt += rtt;
                    successful_measurements += 1;

                    // Record this sample
                    self.latency_stats.add_sample(rtt);
                }
            }
        }

        if successful_measurements > 0 {
            total_rtt / successful_measurements as u32
        } else {
            // Default values
            Duration::from_millis(10)
        }
    }

    /// Intelligent estimation of network delay bounds
    async fn estimate_network_margin(&self) -> Duration {
        let stats = self.latency_stats.get_stats().await;

        if stats.count < 5 {
            // The sample was insufficient and conservative estimates were used
            // Initial value: 50ms + 2 * RTT
            let current_rtt = self.measure_network_rtt().await;
            Duration::from_millis(50) + (current_rtt * 2)
        } else {
            // Intelligent estimation using statistics
            // Network delay bound = P99 delay * 2 + clock drift compensation
            let margin = stats.p99 * 2;

            // It is adjusted according to the network jitter
            let jitter = if stats.max > stats.min {
                stats.max - stats.min
            } else {
                Duration::from_millis(0)
            };

            // Final latency bounds
            margin + jitter / 2 + Duration::from_millis(2) // 2ms basic fault tolerance
        }
    }

    /// Adjust the drift factor dynamically
    async fn calculate_dynamic_drift_factor(&self) -> f64 {
        let stats = self.latency_stats.get_stats().await;

        if stats.count < 10 {
            return 0.01; // Default values
        }

        // The drift factor is adjusted according to the network stability
        let stability_factor: f32 = if stats.p99.as_millis() > 100 {
            // High latency networks, using a larger drift factor
            0.02
        } else if stats.p95.as_millis() < 10 && stats.p99.as_millis() < 20 {
            // Stabilizing low-latency networks, smaller drift factors can be used
            0.005
        } else {
            // General network
            0.01
        };

        // Adjust according to the clock synchronization state
        let clock_sync_factor = if self.check_clock_synchronization().await {
            0.005 // Clocks are well synchronized
        } else {
            0.015 // Clocks may be out of sync
        };

        // Take the larger of the two
        stability_factor.max(clock_sync_factor) as f64
    }

    /// Check the clock synchronization status between nodes
    async fn check_clock_synchronization(&self) -> bool {
        // TODO: In practice, this can be checked with NTP or Redis TIME

        let stats = self.latency_stats.get_stats().await;
        if stats.count < 3 {
            return false; // The data is insufficient, and it is conservative to say that it is out of sync
        }

        // Clocks are considered well synchronized if there is little difference between the maximum and minimum delays
        let latency_range = stats.max - stats.min;
        latency_range < Duration::from_millis(5)
    }

    /// Warm up network latency measurements
    pub async fn warmup_latency_measurement(&self, iterations: usize) {
        info!("Start warming up network latency measurements ({} iterations)...", iterations);

        for i in 0..iterations {
            let rtt = self.measure_network_rtt().await;
            if i == 0 || (i + 1) % 10 == 0 {
                debug!("Warm up iteration {}: RTT = {:? }", i + 1, rtt);
            }
            // Short delay to avoid overload
            sleep(Duration::from_millis(10)).await;
        }

        let stats = self.latency_stats.get_stats().await;
        info!("Delayed network warmup is completed: {}", stats);
    }
}