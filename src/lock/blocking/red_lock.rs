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
use parking_lot::Mutex;
use uuid::Uuid;
use std::time::{Duration, Instant};
use redis::Commands;
use tracing::{debug, info, warn};
use crate::{scripts, LatencyStats, NetworkLatencyStats, RedissonError, RedissonResult, SyncRedisConnectionManager};
use crate::lock::RedLockLocalState;

/// === RRedLock (Red lock) ===
pub struct RRedLock {
    connection_managers: Vec<Arc<SyncRedisConnectionManager>>,
    name: String,
    lease_time: Duration,
    drift_factor: f64,
    local_state: Arc<Mutex<RedLockLocalState>>,
    latency_stats: Arc<NetworkLatencyStats>,
}

impl RRedLock {
    pub fn new(
        connection_managers: Vec<Arc<SyncRedisConnectionManager>>,
        name: String,
        lease_time: Duration,
    ) -> Self {
        Self {
            connection_managers,
            name,
            lease_time,
            drift_factor: 0.01, // Default clock offset factor
            local_state: Arc::new(Mutex::new(RedLockLocalState {
                lock_value: None,
                acquired_at: None,
                acquired_nodes: Vec::new(),
            })),
            latency_stats: Arc::new(NetworkLatencyStats::new(100)),
        }
    }

    pub fn with_drift_factor(mut self, drift_factor: f64) -> Self {
        self.drift_factor = drift_factor;
        self
    }

    /// Acquire locks (following the RedLock algorithm)
    pub fn lock(&self) -> RedissonResult<()> {
        let stats = self.latency_stats.get_stats();
        if stats.count < 3 {
            debug!("Automatically warm up network latency measurements...");
            self.warmup_latency_measurement(5);
        }
        self.lock_with_retries(3, Duration::from_millis(200))
    }

    /// Acquisition lock with retry
    pub fn lock_with_retries(&self, max_retries: u32, retry_delay: Duration) -> RedissonResult<()> {
        for attempt in 0..max_retries {
            match self.try_lock_once() {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if attempt == max_retries - 1 {
                        return Err(e);
                    }
                    // Wait a while and try again
                    std::thread::sleep(retry_delay);
                }
            }
        }
        Err(RedissonError::LockAcquisitionError)
    }

    /// Single attempt to acquire the lock
    fn try_lock_once(&self) -> RedissonResult<()> {
        // Check if the lock is already held
        {
            let state = self.local_state.lock();
            if state.lock_value.is_some() {
                // Lock already held, reentry count processing
                return Ok(());
            }
        }

        let quorum = self.calculate_quorum();
        let lock_value = Uuid::new_v4().to_string();
        let mut successes = 0;
        let mut acquired_nodes = Vec::new();
        let start_time = Instant::now();

        // Phase 1: Try to acquire locks at all nodes
        for (i, manager) in self.connection_managers.iter().enumerate() {
            let acquire_start = Instant::now();

            match manager.get_connection() {
                Ok(mut conn) => {
                    let acquired: i32 = scripts::LOCK_SCRIPT
                        .key(&self.name)
                        .arg(&lock_value)
                        .arg(self.lease_time.as_millis() as i64)
                        .invoke(&mut conn).unwrap_or_else(|_| 0);

                    if acquired > 0 {
                        successes += 1;
                        acquired_nodes.push(i);
                    }
                }
                Err(_) => {
                    // A connection failure is not counted as success
                }
            }

            // Calculate single node acquisition time (for drift calculation)
            let node_elapsed = acquire_start.elapsed();
            if node_elapsed > Duration::from_millis(500) {
                // The single node acquisition time is too long and may be problematic
                warn!("RedLock node {} acquisition took too long: {:?}", i, node_elapsed);
            }
        }
        
        // Phase 2: Calculate the valid time
        let total_elapsed = start_time.elapsed();

        // The clock drift is calculated according to the RedLock paper
        // drift = lease_time * drift_factor + 2ms (Network latency tolerance)
        let _drift = Duration::from_millis(
            (self.drift_factor * self.lease_time.as_millis() as f64) as u64
        ) + Duration::from_millis(2);

        // A modified valid time calculation is used
        let validity_time = self.calculate_validity_time(total_elapsed, successes);
        
        // Stage 3: Judging success
        if successes >= quorum && validity_time.as_millis() > 0 {
            // Lock successfully acquired, state saved
            let mut state = self.local_state.lock();
            state.lock_value = Some(lock_value.clone());
            state.acquired_at = Some(start_time);
            state.acquired_nodes = acquired_nodes;

            // Start the autorenew task (if lease_time > 0)
            if self.lease_time.as_secs() > 0 {
                self.start_renewal_task(lock_value, validity_time);
            }

            Ok(())
        } else {
            // Failure, clean the acquired lock
            self.cleanup_partial_locks(&lock_value, &acquired_nodes);

            if successes < quorum {
                debug!("RedLock failed: only {}/{} nodes acquired", successes, self.connection_managers.len());
                Err(RedissonError::LockAcquisitionError)
            } else {
                debug!("RedLock failed: validity time too short: {:?}", validity_time);
                Err(RedissonError::TimeoutError)
            }
        }
    }

    /// Release a lock
    pub fn unlock(&self) -> RedissonResult<bool> {
        let (lock_value, acquired_nodes) = {
            let mut state = self.local_state.lock();
            let lock_value = state.lock_value.take();
            let acquired_nodes = std::mem::take(&mut state.acquired_nodes);
            state.acquired_at = None;
            (lock_value, acquired_nodes)
        };

        if let Some(lock_value) = lock_value {
            let mut successes = 0;
            let _quorum = self.calculate_quorum();

            // Locks are released only on nodes that have been successfully acquired
            for &node_idx in &acquired_nodes {
                if node_idx < self.connection_managers.len() {
                    if let Ok(mut conn) = self.connection_managers[node_idx].get_connection() {
                        let released: i32 = scripts::UNLOCK_SCRIPT
                            .key(&self.name)
                            .arg(&lock_value)
                            .invoke(&mut conn)
                            .unwrap_or(0);

                        if released > 0 {
                            successes += 1;
                        }
                    }
                }
            }

            // A majority of nodes are required to be released successfully
            let min_releases = (acquired_nodes.len() / 2) + 1;
            Ok(successes >= min_releases)
        } else {
            // No holding lock
            Ok(false)
        }
    }

    /// Attempt to acquire lock (non-blocking)
    pub fn try_lock(&self) -> RedissonResult<bool> {
        match self.try_lock_once() {
            Ok(()) => Ok(true),
            Err(RedissonError::LockAcquisitionError) => Ok(false),
            Err(RedissonError::TimeoutError) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Force unlocking (ignore reentry)
    pub fn force_unlock(&self) -> RedissonResult<bool> {
        // Generate a random value to try to unlock
        let lock_value = Uuid::new_v4().to_string();
        let mut successes = 0;
        let quorum = self.calculate_quorum();

        for manager in &self.connection_managers {
            if let Ok(mut conn) = manager.get_connection() {
                let released: i32 = scripts::UNLOCK_SCRIPT
                    .key(&self.name)
                    .arg(&lock_value)
                    .invoke(&mut conn)
                    .unwrap_or(0);

                if released > 0 {
                    successes += 1;
                }
            }
        }

        // Cleaning up local state
        let mut state = self.local_state.lock();
        state.lock_value = None;
        state.acquired_at = None;
        state.acquired_nodes.clear();

        Ok(successes >= quorum)
    }

    /// Checks whether the current thread holds the lock
    pub fn is_held_by_current_thread(&self) -> bool {
        let state = self.local_state.lock();
        state.lock_value.is_some()
    }

    /// The remaining time to obtain the lock
    pub fn remaining_time(&self) -> RedissonResult<Duration> {
        let state = self.local_state.lock();

        if let (Some(_acquired_at), Some(_lock_value)) = (state.acquired_at, &state.lock_value) {
            // The remaining time of each node is checked and the minimum value is taken
            let mut min_ttl = Duration::from_secs(u64::MAX);

            for &node_idx in &state.acquired_nodes {
                if node_idx < self.connection_managers.len() {
                    if let Ok(mut conn) = self.connection_managers[node_idx].get_connection() {
                        let ttl_ms: i64 = conn.pttl(&self.name).unwrap_or(-1);
                        if ttl_ms > 0 {
                            let node_ttl = Duration::from_millis(ttl_ms as u64);
                            if node_ttl < min_ttl {
                                min_ttl = node_ttl;
                            }
                        }
                    }
                }
            }

            if min_ttl == Duration::from_secs(u64::MAX) {
                Ok(Duration::from_secs(0))
            } else {
                Ok(min_ttl)
            }
        } else {
            Ok(Duration::from_secs(0))
        }
    }

    /// Clean up partially successful locks
    fn cleanup_partial_locks(&self, lock_value: &str, acquired_nodes: &[usize]) {
        for &node_idx in acquired_nodes {
            if node_idx < self.connection_managers.len() {
                if let Ok(mut conn) = self.connection_managers[node_idx].get_connection() {
                    let _ = scripts::UNLOCK_SCRIPT
                        .key(&self.name)
                        .arg(lock_value)
                        .invoke::<i32>(&mut conn);
                }
            }
        }
    }

    /// Start the renewal task
    fn start_renewal_task(&self, lock_value: String, initial_validity: Duration) {
        let connection_managers = self.connection_managers.clone();
        let name = self.name.clone();
        let lease_time = self.lease_time;
        let _drift_factor = self.drift_factor;

        // The renewal interval is set to 1/3 of the valid time
        let renew_interval = initial_validity / 3;

        std::thread::spawn(move || {
            let mut is_running = true;

            while is_running {
                std::thread::sleep(renew_interval);

                // Try to renew at all nodes
                let mut successes = 0;
                for manager in &connection_managers {
                    if let Ok(mut conn) = manager.get_connection() {
                        let renewed: i32 = scripts::RENEW_SCRIPT
                            .key(&name)
                            .arg(&lock_value)
                            .arg(lease_time.as_millis() as i64)
                            .invoke(&mut conn)
                            .unwrap_or(0);

                        if renewed > 0 {
                            successes += 1;
                        }
                    }
                }

                // It is required that most nodes renew successfully
                let quorum = (connection_managers.len() / 2) + 1;
                if successes < quorum {
                    // If the renewal fails, stop the task
                    is_running = false;
                    warn!("RedLock renewal failed: only {}/{} nodes renewed", successes, connection_managers.len());
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
    pub fn healthy_node_count(&self) -> usize {
        self.connection_managers
            .iter()
            .filter(|manager| manager.get_connection().is_ok())
            .count()
    }

    /// Measuring Network Round-trip Time (Ping)
    fn measure_network_rtt(&self) -> Duration {
        let mut total_rtt = Duration::from_secs(0);
        let mut successful_measurements = 0;

        for manager in &self.connection_managers {
            let start = Instant::now();

            // Execute a simple Redis command to measure the RTT
            if let Ok(mut conn) = manager.get_connection() {
                if conn.ping::<String>().is_ok() {
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
    fn estimate_network_margin(&self) -> Duration {
        let stats = self.latency_stats.get_stats();

        if stats.count < 5 {
            // The sample was insufficient and conservative estimates were used
            // Initial value: 50ms + 2 * RTT
            let current_rtt = self.measure_network_rtt();
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
            margin + jitter / 2 + Duration::from_millis(2) // 2ms基础容错
        }
    }

    /// Adjust the drift factor dynamically
    fn calculate_dynamic_drift_factor(&self) -> f64 {
        let stats = self.latency_stats.get_stats();

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
        let clock_sync_factor = if self.check_clock_synchronization() {
            0.005 // Clocks are well synchronized
        } else {
            0.015 // Clocks may be out of sync
        };

        // Take the larger of the two
        stability_factor.max(clock_sync_factor) as f64
    }

    /// Checking clock synchronization status between nodes (simplified implementation)
    fn check_clock_synchronization(&self) -> bool {
        let stats = self.latency_stats.get_stats();
        if stats.count < 3 {
            return false; // The data is insufficient, and it is conservative to say that it is out of sync
        }

        // Clocks are considered well synchronized if there is little difference between the maximum and minimum delays
        let latency_range = stats.max - stats.min;
        latency_range < Duration::from_millis(5)
    }

    /// Warm up network latency measurements
    pub fn warmup_latency_measurement(&self, iterations: usize) {
        info!("Start warming up network latency measurements ({} iterations)...", iterations);

        for i in 0..iterations {
            let rtt = self.measure_network_rtt();
            if i == 0 || (i + 1) % 10 == 0 {
                debug!("Warm up iteration {}: RTT = {:? }", i + 1, rtt);
            }
            // Short delay to avoid overload
            std::thread::sleep(Duration::from_millis(10));
        }

        let stats = self.latency_stats.get_stats();
        info!("Delayed network warmup is completed: {}", stats);
    }

    /// Get the current network latency statistics
    pub fn get_latency_stats(&self) -> LatencyStats {
        self.latency_stats.get_stats()
    }

    /// Calculate more accurate valid times
    fn calculate_validity_time(&self, elapsed: Duration, acquired_nodes: usize) -> Duration {
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
        let drift_factor = self.calculate_dynamic_drift_factor();
        let drift = Duration::from_millis(
            (drift_factor * self.lease_time.as_millis() as f64) as u64
        );

        // Network delay bound
        let network_margin = self.estimate_network_margin();

        // Node health compensation
        let healthy_nodes = self.healthy_node_count();
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
}

impl Clone for RRedLock {
    fn clone(&self) -> Self {
        Self {
            connection_managers: self.connection_managers.clone(),
            name: self.name.clone(),
            lease_time: self.lease_time,
            drift_factor: self.drift_factor,
            local_state: Arc::new(Mutex::new(RedLockLocalState {
                lock_value: None,
                acquired_at: None,
                acquired_nodes: Vec::new(),
            })),
            latency_stats: self.latency_stats.clone(),
        }
    }
}