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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex as TokioMutex};
use tokio::time::interval;

pub struct AsyncLockWatchdog {
    stop_tx: Arc<TokioMutex<Option<watch::Sender<()>>>>,
    task_handle: TokioMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl AsyncLockWatchdog {
    pub fn new() -> Self {
        Self {
            stop_tx: Arc::new(TokioMutex::new(None)),
            task_handle: TokioMutex::new(None),
        }
    }

    pub async fn start<F, Fut>(&mut self, renew_interval: Duration, renew_func: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        // Stop the previous task first
        self.stop().await;

        // Create a stop notification channel
        let (stop_tx, mut stop_rx) = watch::channel(());

        // Storage sender
        *self.stop_tx.lock().await = Some(stop_tx);

        let renew_func = Arc::new(renew_func);

        // Start the watchdog task
        let handle = tokio::spawn({
            let renew_func = renew_func.clone();
            async move {
                let mut interval = interval(renew_interval);

                // The first renewal is performed immediately
                if !renew_func().await {
                    return;
                }

                loop {
                    // Using select! Wait for the interval or stop signal
                    tokio::select! {
                        _ = interval.tick() => {
                            if !renew_func().await {
                                break;
                            }
                        }
                        _ = stop_rx.changed() => {
                            // Stop signal received
                            break;
                        }
                    }
                }
            }
        });

        *self.task_handle.lock().await = Some(handle);
    }

    pub async fn stop(&mut self) {
        // Send a stop signal
        if let Some(stop_tx) = self.stop_tx.lock().await.take() {
            let _ = stop_tx.send(());
        }

        // Waiting for the task to finish
        if let Some(handle) = self.task_handle.lock().await.take() {
            let _ = handle.await;
        }
    }
}