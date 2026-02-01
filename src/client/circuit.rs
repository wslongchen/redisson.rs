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
use std::time::{Duration, Instant};
use tracing::info;

/// Circuit breaker mode
pub struct CircuitBreaker {
    failure_count: u32,
    threshold: u32,
    reset_timeout: Duration,
    last_failure: Option<Instant>,
    state: CircuitState,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, reset_timeout: Duration) -> Self {
        Self {
            failure_count: 0,
            threshold,
            reset_timeout,
            last_failure: None,
            state: CircuitState::Closed,
        }
    }

    pub fn allow_request(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = self.last_failure {
                    if Instant::now().duration_since(last_failure) >= self.reset_timeout {
                        self.state = CircuitState::HalfOpen;
                        info!("Circuit breaker transitioning from Open to HalfOpen");
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    pub fn record_success(&mut self) {
        match self.state {
            CircuitState::HalfOpen => {
                self.state = CircuitState::Closed;
                self.failure_count = 0;
                self.last_failure = None;
            }
            CircuitState::Closed => {
                self.failure_count = 0;
            }
            _ => {}
        }
    }

    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure = Some(Instant::now());

        if self.failure_count >= self.threshold {
            self.state = CircuitState::Open;
        }
    }

    pub fn reset(&mut self) {
        self.failure_count = 0;
        self.last_failure = None;
        self.state = CircuitState::Closed;
    }

    pub fn is_open(&self) -> bool {
        self.state == CircuitState::Open
    }

    pub fn is_closed(&self) -> bool {
        self.state == CircuitState::Closed
    }

    pub fn is_half_open(&self) -> bool {
        self.state == CircuitState::HalfOpen
    }

    pub fn failure_count(&self) -> u32 {
        self.failure_count
    }

    pub fn get_state(&self) -> CircuitState {
        self.state
    }

    pub fn get_last_failure(&self) -> Option<Instant> {
        self.last_failure
    }

    pub fn time_since_last_failure(&self) -> Option<Duration> {
        self.last_failure.map(|last| Instant::now().duration_since(last))
    }
}
