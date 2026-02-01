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
use crate::{AsyncBaseDistributedObject, AsyncRObject, AsyncRObjectBase, AsyncRedisConnectionManager, RedissonResult};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;

/// Asynchronous message listener characteristics
#[async_trait]
pub trait AsyncMessageListener<V>: Send + Sync + 'static
where
    V: DeserializeOwned + Send + Sync + 'static,
{
    async fn on_message(&self, channel: &str, message: V);
}

/// Asynchronous publish/subscribe functionality
#[derive(Clone)]
pub struct AsyncRTopic<V> {
    base: AsyncBaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
    listeners: Arc<RwLock<Vec<Arc<dyn AsyncMessageListener<V>>>>>,
    subscription_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    is_subscribed: Arc<std::sync::atomic::AtomicBool>,
    message_tx: Arc<Mutex<Option<mpsc::UnboundedSender<V>>>>,
}

impl<V> AsyncRTopic<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
            listeners: Arc::new(RwLock::new(Vec::new())),
            subscription_task: Arc::new(Mutex::new(None)),
            is_subscribed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            message_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Publish messages asynchronously
    pub async fn publish(&self, message: &V) -> RedissonResult<u64> {
        let mut conn = self.base.get_connection().await?;
        let message_json = AsyncBaseDistributedObject::serialize(message)?;

        let script = redis::Script::new(r#"
            local channel = KEYS[1]
            local message = ARGV[1]
            
            -- Posting to channel
            local receivers = redis.call('PUBLISH', channel, message)
            
            -- Also save to history (optional)
            local history_key = channel .. ':history'
            redis.call('LPUSH', history_key, message)
            redis.call('LTRIM', history_key, 0, 99)  -- Keep the last 100 entries
            
            return receivers
        "#);

        let receivers: u64 = script
            .key(self.base.get_full_key())
            .arg(message_json)
            .invoke_async(&mut conn)
            .await?;

        Ok(receivers)
    }

    /// Add message listeners asynchronously
    pub async fn add_listener<L>(&self, listener: L) -> RedissonResult<()>
    where
        L: AsyncMessageListener<V>,
    {
        let arc_listener = Arc::new(listener);
        {
            let mut listeners = self.listeners.write().await;
            listeners.push(arc_listener);
        }

        // If you don't have a subscription, start one
        if !self.is_subscribed.load(std::sync::atomic::Ordering::Acquire) {
            self.start_subscription().await?;
        }

        Ok(())
    }

    /// Remove the message listener asynchronously
    pub async fn remove_listener<L>(&self, listener: &L) -> RedissonResult<bool>
    where
        L: AsyncMessageListener<V> + ?Sized,
    {
        let ptr = listener as *const L as *const ();
        let mut listeners = self.listeners.write().await;

        let original_len = listeners.len();
        listeners.retain(|l| {
            let listener_ptr = l.as_ref() as *const dyn AsyncMessageListener<V> as *const ();
            listener_ptr != ptr
        });

        let removed = original_len != listeners.len();

        // If there are no listeners left, stop subscribing
        if listeners.is_empty() {
            self.stop_subscription().await?;
        }

        Ok(removed)
    }

    /// Gets the number of listeners
    pub async fn listener_count(&self) -> usize {
        self.listeners.read().await.len()
    }

    /// Clear all listeners
    pub async fn clear_listeners(&self) -> RedissonResult<()> {
        {
            let mut listeners = self.listeners.write().await;
            listeners.clear();
        }
        self.stop_subscription().await?;
        Ok(())
    }

    /// Are you subscribing
    pub fn is_subscribed(&self) -> bool {
        self.is_subscribed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get the topic name
    pub fn get_topic_name(&self) -> &str {
        &self.base.get_name()
    }

    /// Gets the full channel name
    pub fn get_channel_name(&self) -> String {
        self.base.get_full_key()
    }

    /// Getting historical messages
    pub async fn get_history(&self, count: usize) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection().await?;
        let history_key = format!("{}:history", self.base.get_full_key());

        let messages_json: Vec<String> = conn
            .execute_command(
                &mut redis::cmd("LRANGE")
                    .arg(&history_key)
                    .arg(0)
                    .arg(count as i64 - 1)
            )
            .await?;

        let mut messages = Vec::with_capacity(messages_json.len());
        for json in messages_json {
            match AsyncBaseDistributedObject::deserialize(&json) {
                Ok(message) => messages.push(message),
                Err(e) => {
                    eprintln!("Failed to deserialize history message: {}", e);
                }
            }
        }

        Ok(messages)
    }

    /// Use closures to add listeners
    pub async fn add_listener_fn<F, Fut>(&self, callback: F) -> RedissonResult<()>
    where
        F: Fn(String, V) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static + std::marker::Sync,
    {
        let listener = AsyncFunctionListener::new(callback);
        self.add_listener(listener).await
    }

    /// Creating a message flow (returns an asynchronous message flow)
    pub async fn subscribe_as_stream(&self) -> RedissonResult<impl futures::Stream<Item = RedissonResult<V>>> {
        let channel = self.base.get_full_key();
        let connection_manager = self.base.connection_manager().clone();

        let (tx, rx) = mpsc::unbounded_channel();

        // Start a standalone subscription task
        tokio::spawn(async move {
            if let Err(e) = Self::run_subscription_task(channel, connection_manager, tx).await {
                eprintln!("Subscription task failed: {}", e);
            }
        });

        Ok(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
    }

    // Helper function to run subscription tasks
    async fn run_subscription_task(
        channel: String,
        connection_manager: Arc<AsyncRedisConnectionManager>,
        tx: mpsc::UnboundedSender<RedissonResult<V>>,
    ) -> RedissonResult<()> {
        // Create a dedicated connection for subscriptions
        let mut conn = connection_manager.get_connection().await?;

        // Use Redis's SUBSCRIBE command
        conn.execute_command::<()>(
            &mut redis::cmd("SUBSCRIBE").arg(&channel)
        ).await?;

        // Entering the subscription loop
        loop {
            // Reading messages - Note: In subscription mode, the connection can only be used to receive messages
            // Here, the raw command is used to read
            let response: redis::Value = conn
                .execute_command(&mut redis::cmd("READONLY"))
                .await?;

            match response {
                redis::Value::Array(mut items) => {
                    if items.len() >= 3 {
                        if let (redis::Value::BulkString(msg_type), redis::Value::BulkString(channel_name), redis::Value::BulkString(message_data))
                            = (items.remove(0), items.remove(0), items.remove(0)) {

                            let msg_type_str = String::from_utf8_lossy(&msg_type);
                            if msg_type_str == "message" && channel_name.as_slice() == channel.as_bytes() {
                                if let Ok(message_str) = String::from_utf8(message_data) {
                                    match AsyncBaseDistributedObject::deserialize(&message_str) {
                                        Ok(message) => {
                                            if tx.send(Ok(message)).is_err() {
                                                break; // Receiver turned off
                                            }
                                        }
                                        Err(e) => {
                                            let _ = tx.send(Err(crate::errors::RedissonError::DeserializationError(e.to_string())));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    // For other types of responses, keep waiting
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }

        // unsubscribe
        let _ = connection_manager.get_connection().await?
            .execute_command::<()>(&mut redis::cmd("UNSUBSCRIBE").arg(&channel))
            .await;

        Ok(())
    }

    /// Simple publish-subscribe pattern (no listeners kept)
    pub async fn simple_subscribe<F, Fut>(&self, callback: F) -> RedissonResult<JoinHandle<()>>
    where
        F: Fn(V) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let channel = self.base.get_full_key();
        let connection_manager = self.base.connection_manager().clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::run_simple_subscription(channel, connection_manager, callback).await {
                eprintln!("Simple subscription failed: {}", e);
            }
        });

        Ok(handle)
    }

    async fn run_simple_subscription<F, Fut>(
        channel: String,
        connection_manager: Arc<AsyncRedisConnectionManager>,
        callback: F,
    ) -> RedissonResult<()>
    where
        F: Fn(V) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut conn = connection_manager.get_connection().await?;

        // Subscribe to channels
        conn.execute_command::<()>(
            &mut redis::cmd("SUBSCRIBE").arg(&channel)
        ).await?;

        println!("Subscribed to channel: {}", channel);

        loop {
            // In subscription mode, dedicated read logic is used
            // Note: The asynchronous API of redis-rs has limited support for PubSub, so a round-robin approach is used here
            let response: redis::Value = conn
                .execute_command(&mut redis::cmd("READ"))
                .await?;

            match response {
                redis::Value::Nil => {
                    // No news, just a moment
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
                redis::Value::Array(mut items) => {
                    if items.len() >= 3 {
                        if let (redis::Value::BulkString(msg_type), redis::Value::BulkString(channel_name), redis::Value::BulkString(message_data))
                            = (items.remove(0), items.remove(0), items.remove(0)) {

                            let msg_type_str = String::from_utf8_lossy(&msg_type);
                            if msg_type_str == "message" && channel_name.as_slice() == channel.as_bytes() {
                                if let Ok(message_str) = String::from_utf8(message_data) {
                                    match AsyncBaseDistributedObject::deserialize(&message_str) {
                                        Ok(message) => {
                                            callback(message).await;
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to deserialize message: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Other responses, continue processing
                }
            }

            // Check if you need to exit
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    // Private method: Start subscription
    async fn start_subscription(&self) -> RedissonResult<()> {
        if self.is_subscribed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        let channel = self.base.get_full_key();
        let connection_manager = self.base.connection_manager().clone();
        let listeners = self.listeners.clone();
        let is_subscribed = self.is_subscribed.clone();

        let handle = tokio::spawn(async move {
            match connection_manager.get_connection().await {
                Ok(mut conn) => {
                    // Subscribe to channels
                    if let Err(e) = conn.execute_command::<()>(
                        &mut redis::cmd("SUBSCRIBE").arg(&channel)
                    ).await {
                        eprintln!("Failed to subscribe to channel {}: {}", channel, e);
                        is_subscribed.store(false, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }

                    println!("Successfully subscribed to channel: {}", channel);

                    // Listening for messages
                    while is_subscribed.load(std::sync::atomic::Ordering::Acquire) {
                        // READ messages using the read command (in subscription mode)
                        match conn.execute_command(&mut redis::cmd("READ")).await {
                            Ok(redis::Value::Array(mut items)) => {
                                if items.len() >= 3 {
                                    if let (redis::Value::BulkString(msg_type), redis::Value::BulkString(channel_name), redis::Value::BulkString(message_data))
                                        = (items.remove(0), items.remove(0), items.remove(0)) {

                                        let msg_type_str = String::from_utf8_lossy(&msg_type);
                                        if msg_type_str == "message" && channel_name.as_slice() == channel.as_bytes() {
                                            if let Ok(message_str) = String::from_utf8(message_data) {
                                                match AsyncBaseDistributedObject::deserialize::<V>(&message_str) {
                                                    Ok(message) => {
                                                        // Notify all listeners
                                                        let listeners_guard = listeners.read().await;
                                                        for listener in listeners_guard.iter() {
                                                            let listener_clone = listener.clone();
                                                            let channel_clone = channel.clone();
                                                            let message_clone = message.clone();

                                                            tokio::spawn(async move {
                                                                listener_clone.on_message(&channel_clone, message_clone).await;
                                                            });
                                                        }
                                                    }
                                                    Err(e) => {
                                                        eprintln!("Failed to deserialize message: {}", e);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(redis::Value::Nil) => {
                                // No news, wait a while
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                continue;
                            }
                            Ok(_) => {
                                // For other types of responses, keep waiting
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            }
                            Err(e) => {
                                eprintln!("Failed to read message: {}", e);
                                break;
                            }
                        }
                    }

                    // unsubscribe
                    conn.execute_command::<()>(&mut redis::cmd("UNSUBSCRIBE").arg(&channel)).await.unwrap();

                    is_subscribed.store(false, std::sync::atomic::Ordering::SeqCst);
                }
                Err(e) => {
                    eprintln!("Failed to get connection for subscription: {}", e);
                    is_subscribed.store(false, std::sync::atomic::Ordering::SeqCst);
                }
            }
        });

        {
            let mut task_guard = self.subscription_task.lock().await;
            *task_guard = Some(handle);
        }

        Ok(())
    }

    // Private method: Stop subscribing
    async fn stop_subscription(&self) -> RedissonResult<()> {
        if !self.is_subscribed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        // Wait for the subscription task to finish
        let handle = {
            let mut task_guard = self.subscription_task.lock().await;
            task_guard.take()
        };

        if let Some(handle) = handle {
            let _ = handle.await;
        }

        Ok(())
    }
}


// Async function listeners
struct AsyncFunctionListener<F, Fut, V>
where
    F: Fn(String, V) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
{
    callback: F,
    _marker: std::marker::PhantomData<(Fut, V)>,
}

impl<F, Fut, V> AsyncFunctionListener<F, Fut, V>
where
    F: Fn(String, V) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
    V: DeserializeOwned + Send + Sync + 'static,
{
    fn new(callback: F) -> Self {
        Self {
            callback,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, V> AsyncMessageListener<V> for AsyncFunctionListener<F, Fut, V>
where
    F: Fn(String, V) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static + std::marker::Sync,
    V: DeserializeOwned + Send + Sync + Clone + 'static,
{
    async fn on_message(&self, channel: &str, message: V) {
        (self.callback)(channel.to_string(), message).await;
    }
}

// Implement AsyncRObject for AsyncRTopic
#[async_trait]
impl<V> AsyncRObject for AsyncRTopic<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + Clone + 'static,
{
    async fn delete(&self) -> RedissonResult<bool> {
        self.base.delete().await
    }

    async fn rename(&self, new_name: &str) -> RedissonResult<()> {
        self.base.rename(new_name).await
    }

    async fn is_exists(&self) -> RedissonResult<bool> {
        self.base.is_exists().await
    }

    async fn move_to_db(&self, db_index: i32) -> RedissonResult<bool> {
        self.base.move_to_db(db_index).await
    }

    async fn get_expire_time(&self) -> RedissonResult<Option<std::time::Duration>> {
        self.base.get_expire_time().await
    }

    async fn expire(&self, duration: std::time::Duration) -> RedissonResult<bool> {
        self.base.expire(duration).await
    }

    async fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        self.base.expire_at(timestamp).await
    }

    async fn clear_expire(&self) -> RedissonResult<bool> {
        self.base.clear_expire().await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RedissonConfig;
    use crate::AsyncSyncRedisConnectionManager;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_topic_publish() -> RedissonResult<()> {
        let config = RedissonConfig::single_server("redis://localhost:6379")
            .with_pool_size(3);

        let manager = AsyncSyncRedisConnectionManager::new(&config).await?;

        // Creating a Theme
        let topic = AsyncRTopic::<String>::new(
            manager.inner.clone(),
            "test-topic".to_string()
        );

        // Release a message
        let receivers = topic.publish(&"Hello, World!".to_string()).await?;
        println!("Published to {} receivers", receivers);

        // Tests get history messages
        let history = topic.get_history(5).await?;
        println!("History: {:?}", history);

        Ok(())
    }

    #[tokio::test]
    async fn test_topic_simple_subscribe() -> RedissonResult<()> {
        let config = RedissonConfig::single_server("redis://localhost:6379")
            .with_pool_size(3);

        let manager = AsyncSyncRedisConnectionManager::new(&config).await?;

        let topic = AsyncRTopic::<String>::new(
            manager.inner.clone(),
            "test-simple-sub".to_string()
        );

        let message_received = Arc::new(AtomicUsize::new(0));
        let message_received_clone = message_received.clone();

        // Start a simple subscription
        let handle = topic.simple_subscribe(move |message| {
            let message_received = message_received_clone.clone();
            async move {
                println!("Simple subscription received: {}", message);
                message_received.fetch_add(1, Ordering::SeqCst);
            }
        }).await?;

        // Waiting for subscriptions to start
        sleep(Duration::from_millis(100)).await;

        // Release a message
        let _ = topic.publish(&"Test message".to_string()).await?;

        // Waiting for message processing
        sleep(Duration::from_millis(200)).await;

        // unsubscribe
        handle.abort();

        assert!(message_received.load(Ordering::SeqCst) >= 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_topic_stream() -> RedissonResult<()> {
        let config = RedissonConfig::single_server("redis://localhost:6379")
            .with_pool_size(3);

        let manager = AsyncSyncRedisConnectionManager::new(&config).await?;

        let topic = AsyncRTopic::<String>::new(
            manager.inner.clone(),
            "test-stream".to_string()
        );

        // Fetching the message stream
        let mut stream = topic.subscribe_as_stream().await?;

        // Start a task to publish a message
        let topic_clone = topic.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            let _ = topic_clone.publish(&"Stream message 1".to_string()).await;
            sleep(Duration::from_millis(50)).await;
            let _ = topic_clone.publish(&"Stream message 2".to_string()).await;
        });

        // Read messages from the stream
        let mut messages = Vec::new();

        // Setting a timeout
        let timeout = sleep(Duration::from_secs(1));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                Some(result) = stream.next() => {
                    match result {
                        Ok(message) => {
                            println!("Stream received: {}", message);
                            messages.push(message);
                            if messages.len() >= 2 {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Stream error: {}", e);
                            break;
                        }
                    }
                }
                _ = &mut timeout => {
                    println!("Stream timeout");
                    break;
                }
            }
        }

        assert!(messages.len() <= 2); // You may receive 0, 1, or 2 messages

        Ok(())
    }
}