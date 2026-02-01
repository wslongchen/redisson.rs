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
use std::thread;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use serde::{Serialize, de::DeserializeOwned};
use parking_lot::Mutex;
use redis::PubSub;
use crate::{scripts, BaseDistributedObject, RObject, RObjectBase, RedissonResult, SyncRedisConnectionManager};

/// Message listener characteristics
pub trait MessageListener<V>: Send + Sync + 'static
where
    V: DeserializeOwned + Send + Sync + 'static,
{
    fn on_message(&self, channel: &str, message: V);
}

/// Publish/subscribe functionality
#[derive(Clone)]
pub struct RTopic<V> {
    base: BaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
    listeners: Arc<Mutex<Vec<Arc<dyn MessageListener<V>>>>>,
    is_subscribed: Arc<AtomicBool>,
    subscription_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

impl<V> RTopic<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static + Clone,
{
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
            listeners: Arc::new(Mutex::new(Vec::new())),
            is_subscribed: Arc::new(AtomicBool::new(false)),
            subscription_thread: Arc::new(Mutex::new(None)),
        }
    }

    /// Release a message
    pub fn publish(&self, message: &V) -> RedissonResult<u64> {
        let mut conn = self.base.get_connection()?;
        let message_json = BaseDistributedObject::serialize(message)?;

        let receivers: u64 = scripts::PUBLISH_SCRIPT
            .key(self.base.get_full_key())
            .arg(message_json)
            .invoke(&mut conn)?;

        Ok(receivers)
    }

    /// Add a message listener
    pub fn add_listener<L>(&self, listener: L) -> RedissonResult<()>
    where
        L: MessageListener<V>,
    {
        let arc_listener = Arc::new(listener);
        {
            let mut listeners = self.listeners.lock();
            listeners.push(arc_listener);
        }

        // If you don't have a subscription, start one
        if !self.is_subscribed.load(Ordering::Acquire) {
            self.start_subscription()?;
        }

        Ok(())
    }

    /// Remove the message listener
    pub fn remove_listener<L>(&self, listener: &L) -> RedissonResult<bool>
    where
        L: MessageListener<V> + ?Sized,
    {
        let ptr = listener as *const L as *const ();
        let mut listeners = self.listeners.lock();

        let original_len = listeners.len();
        listeners.retain(|l| {
            let listener_ptr = l.as_ref() as *const dyn MessageListener<V> as *const ();
            listener_ptr != ptr
        });

        let removed = original_len != listeners.len();

        // 如果没有监听器了，停止订阅
        if listeners.is_empty() {
            self.stop_subscription()?;
        }

        Ok(removed)
    }

    /// Gets the number of listeners
    pub fn listener_count(&self) -> usize {
        self.listeners.lock().len()
    }

    /// Clear all listeners
    pub fn clear_listeners(&self) -> RedissonResult<()> {
        {
            let mut listeners = self.listeners.lock();
            listeners.clear();
        }
        self.stop_subscription()?;
        Ok(())
    }

    /// Are you subscribing
    pub fn is_subscribed(&self) -> bool {
        self.is_subscribed.load(Ordering::Acquire)
    }

    /// Get the topic name
    pub fn get_topic_name(&self) -> &str {
        &self.base.get_name()
    }

    /// Gets the full channel name
    pub fn get_channel_name(&self) -> String {
        self.base.get_full_key()
    }

    // Private method: Start subscription
    fn start_subscription(&self) -> RedissonResult<()> {
        if self.is_subscribed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let channel = self.base.get_full_key();
        let connection_manager = self.base.connection_manager().clone();
        let listeners = self.listeners.clone();
        let is_subscribed = self.is_subscribed.clone();

        let handle = thread::spawn(move || {
            // Get a new connection to subscribe to
            match connection_manager.get_connection() {
                Ok(mut conn) => {
                    // Convert to a PubSub connection
                    let mut pubsub_conn = conn.as_pubsub().unwrap();

                    // Subscribe to channels
                    if let Err(e) = pubsub_conn.subscribe(&channel) {
                        eprintln!("Failed to subscribe to channel {}: {}", channel, e);
                        is_subscribed.store(false, Ordering::SeqCst);
                        return;
                    }

                    // Listening for messages
                    while is_subscribed.load(Ordering::Acquire) {
                        match pubsub_conn.get_message() {
                            Ok(msg) => {
                                if let Ok(payload) = msg.get_payload::<String>() {
                                    // Deserialize the message
                                    let message_result: Result<V, _> = BaseDistributedObject::deserialize(&payload);
                                    match message_result {
                                        Ok(message) => {
                                            // 通知所有监听器
                                            let listeners_guard = listeners.lock();
                                            for listener in listeners_guard.iter() {
                                                listener.on_message(&channel, message.clone());
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to deserialize message: {}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to get message: {}", e);
                                break;
                            }
                        }
                    }

                    // unsubscribe
                    let _ = pubsub_conn.unsubscribe(&channel);
                    is_subscribed.store(false, Ordering::SeqCst);
                }
                Err(e) => {
                    eprintln!("Failed to get connection for subscription: {}", e);
                    is_subscribed.store(false, Ordering::SeqCst);
                }
            }
        });

        {
            let mut thread_guard = self.subscription_thread.lock();
            *thread_guard = Some(handle);
        }

        Ok(())
    }

    // Private method: Stop subscribing
    fn stop_subscription(&self) -> RedissonResult<()> {
        if !self.is_subscribed.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        // Wait for the subscriber thread to end
        let handle = {
            let mut thread_guard = self.subscription_thread.lock();
            thread_guard.take()
        };

        if let Some(handle) = handle {
            let _ = handle.join();
        }

        Ok(())
    }
}

// Implement simple function listeners
pub struct FunctionListener<F, V>
where
    F: Fn(String, V) + Send + Sync + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
{
    callback: F,
    _marker: std::marker::PhantomData<V>,
}

impl<F, V> FunctionListener<F, V>
where
    F: Fn(String, V) + Send + Sync + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(callback: F) -> Self {
        Self {
            callback,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<F, V> MessageListener<V> for FunctionListener<F, V>
where
    F: Fn(String, V) + Send + Sync + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
{
    fn on_message(&self, channel: &str, message: V) {
        (self.callback)(channel.to_string(), message);
    }
}

// Implement convenience methods for closures
impl<V> RTopic<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static + Clone,
{
    /// Use closures to add listeners
    pub fn add_listener_fn<F>(&self, callback: F) -> RedissonResult<()>
    where
        F: Fn(String, V) + Send + Sync + 'static,
    {
        let listener = FunctionListener::new(callback);
        self.add_listener(listener)
    }
}

// Implement RObjectBase
impl<V> crate::RObjectBase<String> for RTopic<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static + Clone,
{
    fn get_connection(&self) -> RedissonResult<crate::RedisConnection> {
        self.base.get_connection()
    }

    fn serialize_value(&self, value: &String) -> RedissonResult<String> {
        Ok(value.clone())
    }

    fn deserialize_value(&self, data: &str) -> RedissonResult<String> {
        Ok(data.to_string())
    }

    fn get_full_key(&self) -> String {
        self.base.get_full_key()
    }
}

// Implementing RObject
impl<V> crate::RObject for RTopic<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static + Clone,
{
    fn get_name(&self) -> &str {
        self.get_topic_name()
    }

    fn delete(&self) -> RedissonResult<bool> {
        self.base.delete()
    }

    fn rename(&self, new_name: &str) -> RedissonResult<()> {
        self.base.rename(new_name)
    }

    fn is_exists(&self) -> RedissonResult<bool> {
        self.base.is_exists()
    }

    fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        self.base.move_to_db(db_index)
    }

    fn get_expire_time(&self) -> RedissonResult<Option<std::time::Duration>> {
        self.base.get_expire_time()
    }

    fn expire(&self, duration: std::time::Duration) -> RedissonResult<bool> {
        self.base.expire(duration)
    }

    fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        self.base.expire_at(timestamp)
    }

    fn clear_expire(&self) -> RedissonResult<bool> {
        self.base.clear_expire()
    }
}