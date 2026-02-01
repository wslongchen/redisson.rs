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
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::sync::atomic::AtomicU32;
use std::time::Duration;
use std::thread;
use parking_lot::Mutex;

pub struct LockWatchdog {
    should_stop: Arc<AtomicBool>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    epoch: Arc<AtomicU32>,
}

impl LockWatchdog {
    pub fn new() -> Self {
        Self {
            should_stop: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
            epoch: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn start<F>(&mut self, renew_interval: Duration, renew_func: F)
    where
        F: Fn() -> bool + Send + 'static,
    {
        // Stop the guard dog first
        self.stop();

        // Reset the stop flag
        self.should_stop.store(false, Ordering::SeqCst);

        // Increment the version number so that the old watchdog loop can exit quickly
        let current_epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;

        let should_stop = self.should_stop.clone();
        let epoch = self.epoch.clone();

        let handle = thread::spawn(move || {
            while !should_stop.load(Ordering::SeqCst) {
                // Check if it has been replaced by the new version
                if epoch.load(Ordering::SeqCst) != current_epoch {
                    break;
                }

                // The first renewal is performed immediately
                if !renew_func() {
                    // Renewal failed, quit
                    break;
                }

                // Use more accurate sleep to respond to stop signals in a timely manner
                let sleep_duration = renew_interval;
                let mut slept = Duration::from_secs(0);
                while slept < sleep_duration && !should_stop.load(Ordering::SeqCst) {
                    let remaining = sleep_duration - slept;
                    let chunk = remaining.min(Duration::from_millis(100));
                    thread::sleep(chunk);
                    slept += chunk;

                    // Double-check the version
                    if epoch.load(Ordering::SeqCst) != current_epoch {
                        break;
                    }
                }

                // Before the loop ends, the version is checked again
                if epoch.load(Ordering::SeqCst) != current_epoch {
                    break;
                }
            }
        });

        *self.handle.lock() = Some(handle);
    }

    pub fn stop(&mut self) {
        // Set the stop flag
        self.should_stop.store(true, Ordering::SeqCst);

        // Incrementing the version number ensures that any running loops exit
        self.epoch.fetch_add(1, Ordering::SeqCst);

        // Waiting for the thread to end
        let handle = self.handle.lock().take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }

        // Reset the stop flag to prepare for the next start
        self.should_stop.store(false, Ordering::SeqCst);
    }
}

impl Drop for LockWatchdog {
    fn drop(&mut self) {
        self.stop();
    }
}
