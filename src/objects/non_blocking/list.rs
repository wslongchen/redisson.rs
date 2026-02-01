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
use std::time::Duration;
use serde::de::DeserializeOwned;
use serde::Serialize;
use async_trait::async_trait;
use crate::{RedissonResult, AsyncRedisConnectionManager, AsyncRedisConnection, AsyncBaseDistributedObject, AsyncRObject, AsyncRLockable, AsyncRLock, AsyncRFairLock, AsyncRObjectBase};

// === AsyncRList Asynchronous list ===
pub struct AsyncRList<V> {
    base: AsyncBaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
    trim_enabled: bool,
    trim_size: Option<usize>,
}

impl<V> AsyncRList<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(connection_manager: Arc<AsyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: AsyncBaseDistributedObject::new(connection_manager, name),
            _marker: std::marker::PhantomData,
            trim_enabled: false,
            trim_size: None,
        }
    }

    pub fn with_trim(mut self, max_size: usize) -> Self {
        self.trim_enabled = true;
        self.trim_size = Some(max_size);
        self
    }

    // ============ Basic operations ============

    /// Adding elements to the end of a list asynchronously (same as add_last)
    pub async fn add(&self, value: &V) -> RedissonResult<bool> {
        self.add_last(value).await
    }

    /// Adds an element to the head of the list asynchronously
    pub async fn add_first(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let len: i32 = conn
            .execute_command(&mut redis::cmd("LPUSH").arg(self.base.get_full_key()).arg(value_json))
            .await?;

        self.maybe_trim(&mut conn).await?;
        Ok(len > 0)
    }

    /// Adds items to the end of the list asynchronously
    pub async fn add_last(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let len: i32 = conn
            .execute_command(&mut redis::cmd("RPUSH").arg(self.base.get_full_key()).arg(value_json))
            .await?;

        self.maybe_trim(&mut conn).await?;
        Ok(len > 0)
    }

    /// Inserts before the specified element asynchronously
    pub async fn add_before(&self, pivot: &V, value: &V) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection().await?;
        let pivot_json = AsyncBaseDistributedObject::serialize(pivot)?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let len: i64 = conn
            .execute_command(&mut redis::cmd("LINSERT")
                .arg(self.base.get_full_key())
                .arg("BEFORE")
                .arg(pivot_json)
                .arg(value_json))
            .await?;

        if len > 0 {
            self.maybe_trim(&mut conn).await?;
        }

        Ok(len)
    }

    /// Inserts after the specified element asynchronously
    pub async fn add_after(&self, pivot: &V, value: &V) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection().await?;
        let pivot_json = AsyncBaseDistributedObject::serialize(pivot)?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let len: i64 = conn
            .execute_command(&mut redis::cmd("LINSERT")
                .arg(self.base.get_full_key())
                .arg("AFTER")
                .arg(pivot_json)
                .arg(value_json))
            .await?;

        if len > 0 {
            self.maybe_trim(&mut conn).await?;
        }

        Ok(len)
    }

    /// Async bulk added to the head of the list
    pub async fn add_all_first(&self, values: &[V]) -> RedissonResult<usize> {
        if values.is_empty() {
            return Ok(0);
        }

        let mut conn = self.base.get_connection().await?;
        let values_json: Vec<String> = values.iter()
            .map(|v| AsyncBaseDistributedObject::serialize(v))
            .collect::<Result<_, _>>()?;

        let mut cmd = redis::cmd("LPUSH");
        cmd.arg(self.base.get_full_key());
        for value_json in &values_json {
            cmd.arg(value_json);
        }

        let len: i32 = conn.execute_command(&mut cmd).await?;
        self.maybe_trim(&mut conn).await?;
        Ok(len as usize)
    }

    /// Async bulk is added to the end of the list
    pub async fn add_all_last(&self, values: &[V]) -> RedissonResult<usize> {
        if values.is_empty() {
            return Ok(0);
        }

        let mut conn = self.base.get_connection().await?;
        let values_json: Vec<String> = values.iter()
            .map(|v| AsyncBaseDistributedObject::serialize(v))
            .collect::<Result<_, _>>()?;

        let mut cmd = redis::cmd("RPUSH");
        cmd.arg(self.base.get_full_key());
        for value_json in &values_json {
            cmd.arg(value_json);
        }

        let len: i32 = conn.execute_command(&mut cmd).await?;
        self.maybe_trim(&mut conn).await?;
        Ok(len as usize)
    }

    /// Asynchronous bulk add (automatic location selection)
    pub async fn add_all(&self, values: &[V], to_front: bool) -> RedissonResult<usize> {
        if to_front {
            self.add_all_first(values).await
        } else {
            self.add_all_last(values).await
        }
    }

    // ============ Remove operation ============

    /// Remove items from the head of a list asynchronously (pop_front)
    pub async fn pop_front(&self) -> RedissonResult<Option<V>> {
        self.pop_first().await
    }

    /// Removes items from the head of the list asynchronously
    pub async fn pop_first(&self) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;
        let result: Option<String> = conn
            .execute_command(&mut redis::cmd("LPOP").arg(self.base.get_full_key()))
            .await?;

        match result {
            Some(value_json) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Removes multiple items from the head of a list asynchronously
    pub async fn pop_first_n(&self, count: usize) -> RedissonResult<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.base.get_connection().await?;
        let mut cmd = redis::cmd("LPOP");
        cmd.arg(self.base.get_full_key()).arg(count);

        let results: Vec<String> = conn.execute_command(&mut cmd).await?;

        let mut values = Vec::with_capacity(results.len());
        for value_json in results {
            let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
            values.push(value);
        }

        Ok(values)
    }

    /// Removes items from the end of the list asynchronously
    pub async fn pop_last(&self) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;
        let result: Option<String> = conn
            .execute_command(&mut redis::cmd("RPOP").arg(self.base.get_full_key()))
            .await?;

        match result {
            Some(value_json) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Removes multiple items from the end of a list asynchronously
    pub async fn pop_last_n(&self, count: usize) -> RedissonResult<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.base.get_connection().await?;
        let mut cmd = redis::cmd("RPOP");
        cmd.arg(self.base.get_full_key()).arg(count);

        let results: Vec<String> = conn.execute_command(&mut cmd).await?;

        let mut values = Vec::with_capacity(results.len());
        for value_json in results {
            let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
            values.push(value);
        }

        Ok(values)
    }

    /// Removes the specified element asynchronously (removes the first match by default)
    pub async fn remove(&self, value: &V) -> RedissonResult<bool> {
        self.remove_count(value, 1).await.map(|count| count > 0)
    }

    /// Removes the specified element a specified number of times asynchronously
    pub async fn remove_count(&self, value: &V, count: i64) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection().await?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        let removed: i64 = conn
            .execute_command(&mut redis::cmd("LREM")
                .arg(self.base.get_full_key())
                .arg(count)
                .arg(value_json))
            .await?;

        Ok(removed)
    }

    /// Remove all matches asynchronously
    pub async fn remove_all(&self, value: &V) -> RedissonResult<i64> {
        self.remove_count(value, 0).await // count为0表示移除所有
    }

    /// Removes elements by index asynchronously
    pub async fn remove_by_index(&self, index: i64) -> RedissonResult<Option<V>> {
        let script = redis::Script::new(r#"
            local key = KEYS[1]
            local index = tonumber(ARGV[1])
            
            -- Gets the element to remove
            local value = redis.call('LINDEX', key, index)
            if not value then
                return nil
            end
            
            -- With LREM deletion, a unique tag is inserted first
            local temp = '__TEMP__' .. tostring(redis.call('TIME')[1])
            redis.call('LSET', key, index, temp)
            redis.call('LREM', key, 1, temp)
            
            return value
        "#);

        let mut conn = self.base.get_connection().await?;
        let result: Option<String> = script
            .key(self.base.get_full_key())
            .arg(index)
            .invoke_async(&mut conn)
            .await?;

        match result {
            Some(value_json) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Asynchronously removes elements by range
    pub async fn remove_range(&self, start: i64, end: i64) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection().await?;

        // We start by getting the elements in the range
        let values_json: Vec<String> = conn
            .execute_command(&mut redis::cmd("LRANGE")
                .arg(self.base.get_full_key())
                .arg(start)
                .arg(end))
            .await?;

        if values_json.is_empty() {
            return Ok(Vec::new());
        }

        // deserialize
        let mut values = Vec::with_capacity(values_json.len());
        for value_json in &values_json {
            let value: V = AsyncBaseDistributedObject::deserialize(value_json)?;
            values.push(value);
        }

        // Use a script to handle removal
        let script = redis::Script::new(r#"
            local key = KEYS[1]
            local start = tonumber(ARGV[1])
            local end = tonumber(ARGV[2])
            
            if start == 0 then
                -- Remove the section from head to end
                local new_start = end + 1
                redis.call('LTRIM', key, new_start, -1)
            elseif end == -1 then
                -- Remove the part from start to end
                redis.call('LTRIM', key, 0, start - 1)
            else
                -- Complex case: We need to remove the middle part
                local left_part = redis.call('LRANGE', key, 0, start - 1)
                local right_part = redis.call('LRANGE', key, end + 1, -1)
                
                -- Rebuilding the list
                redis.call('DEL', key)
                if #left_part > 0 then
                    redis.call('RPUSH', key, unpack(left_part))
                end
                if #right_part > 0 then
                    redis.call('RPUSH', key, unpack(right_part))
                end
            end
            
            return #KEYS[1]  -- Return key (placeholder)
        "#);

        let _: String = script
            .key(self.base.get_full_key())
            .arg(start)
            .arg(end)
            .invoke_async(&mut conn)
            .await?;

        Ok(values)
    }

    /// Remove and return the first element asynchronously (blocking version)
    pub async fn bpop_first(&self, timeout: Duration) -> RedissonResult<Option<(String, V)>> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("BLPOP");
        cmd.arg(self.base.get_full_key()).arg(timeout.as_secs() as f64);

        let result: Option<(String, String)> = conn.execute_command(&mut cmd).await?;

        match result {
            Some((_key, value_json)) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some((self.base.get_full_key(), value)))
            }
            None => Ok(None),
        }
    }

    /// Removes and returns the last element asynchronously (blocking version)
    pub async fn bpop_last(&self, timeout: Duration) -> RedissonResult<Option<(String, V)>> {
        let mut conn = self.base.get_connection().await?;

        let mut cmd = redis::cmd("BRPOP");
        cmd.arg(self.base.get_full_key()).arg(timeout.as_secs() as f64);

        let result: Option<(String, String)> = conn.execute_command(&mut cmd).await?;

        match result {
            Some((_key, value_json)) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some((self.base.get_full_key(), value)))
            }
            None => Ok(None),
        }
    }

    /// Async pops from one list and pushes to another
    pub async fn pop_and_push(&self, other_list_key: &str, from_right: bool) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;

        let result: Option<String> = if from_right {
            conn.execute_command(&mut redis::cmd("RPOPLPUSH")
                .arg(self.base.get_full_key())
                .arg(other_list_key))
                .await?
        } else {
            conn.execute_command(&mut redis::cmd("LPOPRPUSH")
                .arg(self.base.get_full_key())
                .arg(other_list_key))
                .await?
        };

        match result {
            Some(value_json) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Asynchronous blocking version: Popping from one list and pushing to another
    pub async fn bpop_and_push(&self, other_list_key: &str, timeout: Duration, from_right: bool) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;

        if from_right {
            let mut cmd = redis::cmd("BRPOPLPUSH");
            cmd.arg(self.base.get_full_key())
                .arg(other_list_key)
                .arg(timeout.as_secs() as f64);

            let result: Option<String> = conn.execute_command(&mut cmd).await?;

            match result {
                Some(value_json) => {
                    let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                    Ok(Some(value))
                }
                None => Ok(None),
            }
        } else {
            // Redis does not have BLPOPRPUSH and is implemented using scripts
            let script = redis::Script::new(r#"
                local source = KEYS[1]
                local dest = KEYS[2]
                local timeout = tonumber(ARGV[1])
                
                local value = redis.call('BLPOP', source, timeout)
                if value then
                    value = value[2]  -- BLPOP返回的是[key, value]
                    redis.call('LPUSH', dest, value)
                    return value
                end
                return nil
            "#);

            let result: Option<String> = script
                .key(self.base.get_full_key())
                .key(other_list_key)
                .arg(timeout.as_secs() as usize)
                .invoke_async(&mut conn)
                .await?;

            match result {
                Some(value_json) => {
                    let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                    Ok(Some(value))
                }
                None => Ok(None),
            }
        }
    }

    // ============ Query operations ============

    /// Retrieve elements by index asynchronously
    pub async fn get(&self, index: i64) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection().await?;
        let result: Option<String> = conn
            .execute_command(&mut redis::cmd("LINDEX").arg(self.base.get_full_key()).arg(index))
            .await?;

        match result {
            Some(value_json) => {
                let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Sets elements by index asynchronously
    pub async fn set(&self, index: i64, value: &V) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        let value_json = AsyncBaseDistributedObject::serialize(value)?;

        conn.execute_command::<()>(&mut redis::cmd("LSET")
            .arg(self.base.get_full_key())
            .arg(index)
            .arg(value_json))
            .await?;
        Ok(())
    }

    /// Get the list size asynchronously
    pub async fn size(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection().await?;
        let len: i32 = conn
            .execute_command(&mut redis::cmd("LLEN").arg(self.base.get_full_key()))
            .await?;

        Ok(len as usize)
    }

    /// Asynchronously checks whether the list is empty
    pub async fn is_empty(&self) -> RedissonResult<bool> {
        Ok(self.size().await? == 0)
    }

    /// Asynchronously checks whether the specified element is contained
    pub async fn contains(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection().await?;
        let values: Vec<String> = conn
            .execute_command(&mut redis::cmd("LRANGE").arg(self.base.get_full_key()).arg(0).arg(-1))
            .await?;

        let target_json = AsyncBaseDistributedObject::serialize(value)?;
        for value_json in values {
            if value_json == target_json {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get the element index (first match) asynchronously
    pub async fn index_of(&self, value: &V) -> RedissonResult<Option<i64>> {
        self.index_of_from(value, 0).await
    }

    /// Looks up the element index asynchronously from the specified position
    pub async fn index_of_from(&self, value: &V, start_index: i64) -> RedissonResult<Option<i64>> {
        let target_json = AsyncBaseDistributedObject::serialize(value)?;
        let values = self.range(start_index, -1).await?;

        for (i, value) in values.iter().enumerate() {
            let value_json = AsyncBaseDistributedObject::serialize(value)?;
            if value_json == target_json {
                return Ok(Some(start_index + i as i64));
            }
        }

        Ok(None)
    }

    /// Retrieve the index of the last match asynchronously
    pub async fn last_index_of(&self, value: &V) -> RedissonResult<Option<i64>> {
        let target_json = AsyncBaseDistributedObject::serialize(value)?;
        let values = self.range(0, -1).await?;

        for (i, value) in values.iter().enumerate().rev() {
            let value_json = AsyncBaseDistributedObject::serialize(value)?;
            if value_json == target_json {
                return Ok(Some(i as i64));
            }
        }

        Ok(None)
    }

    /// Fetch range elements asynchronously
    pub async fn range(&self, start: i64, end: i64) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection().await?;
        let values_json: Vec<String> = conn
            .execute_command(&mut redis::cmd("LRANGE").arg(self.base.get_full_key()).arg(start).arg(end))
            .await?;

        let mut values = Vec::with_capacity(values_json.len());
        for value_json in values_json {
            let value: V = AsyncBaseDistributedObject::deserialize(&value_json)?;
            values.push(value);
        }

        Ok(values)
    }

    /// Fetch all elements asynchronously
    pub async fn all(&self) -> RedissonResult<Vec<V>> {
        self.range(0, -1).await
    }

    /// Get the first N elements asynchronously
    pub async fn first_n(&self, n: usize) -> RedissonResult<Vec<V>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        self.range(0, (n - 1) as i64).await
    }

    /// Get the last N elements asynchronously
    pub async fn last_n(&self, n: usize) -> RedissonResult<Vec<V>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let size = self.size().await?;
        if n >= size {
            return self.all().await;
        }
        self.range((size - n) as i64, -1).await
    }

    /// Get the first element asynchronously
    pub async fn first(&self) -> RedissonResult<Option<V>> {
        self.get(0).await
    }

    /// Get the last element asynchronously
    pub async fn last(&self) -> RedissonResult<Option<V>> {
        self.get(-1).await
    }

    // ============ Modify operations ============

    /// Trim the list asynchronously
    pub async fn trim(&self, start: i64, end: i64) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        conn.execute_command::<()>(&mut redis::cmd("LTRIM")
            .arg(self.base.get_full_key())
            .arg(start)
            .arg(end))
            .await?;
        Ok(())
    }

    /// Clear the list asynchronously
    pub async fn clear(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection().await?;
        conn.execute_command::<()>(&mut redis::cmd("DEL").arg(self.base.get_full_key()))
            .await?;
        Ok(())
    }

    /// The first N elements are kept asynchronously
    pub async fn keep_first(&self, n: usize) -> RedissonResult<()> {
        if n == 0 {
            self.clear().await
        } else {
            self.trim(0, (n - 1) as i64).await
        }
    }

    /// The last N elements are kept asynchronously
    pub async fn keep_last(&self, n: usize) -> RedissonResult<()> {
        let size = self.size().await?;
        if n == 0 {
            self.clear().await
        } else if n >= size {
            Ok(())
        } else {
            self.trim((size - n) as i64, -1).await
        }
    }

    /// Remove the first N elements asynchronously
    pub async fn remove_first_n(&self, n: usize) -> RedissonResult<Vec<V>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        self.remove_range(0, (n - 1) as i64).await
    }

    /// The last N elements are removed asynchronously
    pub async fn remove_last_n(&self, n: usize) -> RedissonResult<Vec<V>> {
        let size = self.size().await?;
        if n == 0 {
            return Ok(Vec::new());
        }
        if n >= size {
            return self.all().await;
        }
        self.remove_range((size - n) as i64, -1).await
    }

    /// Asynchronous fast setting (no return value)
    pub async fn fast_set(&self, index: i64, value: &V) -> RedissonResult<()> {
        self.set(index, value).await
    }

    /// Asynchronous fast removal (no return value)
    pub async fn fast_remove(&self, value: &V) -> RedissonResult<bool> {
        self.remove(value).await
    }

    // ============ Utility methods ============

    /// Convert the list to Vec asynchronously
    pub async fn to_vec(&self) -> RedissonResult<Vec<V>> {
        self.all().await
    }

    /// Create a sublist asynchronously (without modifying the original)
    pub async fn sub_list(&self, start: i64, end: i64) -> RedissonResult<Vec<V>> {
        self.range(start, end).await
    }

    /// Asynchronously checks whether the list contains all the specified elements
    pub async fn contains_all(&self, values: &[V]) -> RedissonResult<bool> {
        let list_values = self.all().await?;

        for target in values {
            let target_json = AsyncBaseDistributedObject::serialize(target)?;
            let mut found = false;

            for value in &list_values {
                let value_json = AsyncBaseDistributedObject::serialize(value)?;
                if value_json == target_json {
                    found = true;
                    break;
                }
            }

            if !found {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Asynchronously checks whether the list contains any of the specified elements
    pub async fn contains_any(&self, values: &[V]) -> RedissonResult<bool> {
        let list_values = self.all().await?;

        for target in values {
            let target_json = AsyncBaseDistributedObject::serialize(target)?;

            for value in &list_values {
                let value_json = AsyncBaseDistributedObject::serialize(value)?;
                if value_json == target_json {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Add an element asynchronously if it doesn't exist
    pub async fn add_if_absent(&self, value: &V) -> RedissonResult<bool> {
        if self.contains(value).await? {
            Ok(false)
        } else {
            self.add_last(value).await
        }
    }

    /// Async bulk add elements if they don't exist
    pub async fn add_all_if_absent(&self, values: &[V]) -> RedissonResult<usize> {
        let mut added = 0;

        for value in values {
            if !self.contains(value).await? {
                self.add_last(value).await?;
                added += 1;
            }
        }

        Ok(added)
    }

    /// Create a list iterator asynchronously
    pub fn iter(&self) -> AsyncRListIterator<'_, V> {
        AsyncRListIterator {
            list: self,
            current_index: 0,
        }
    }

    async fn maybe_trim(&self, conn: &mut AsyncRedisConnection) -> RedissonResult<()> {
        if self.trim_enabled {
            if let Some(max_size) = self.trim_size {
                let current_len: i32 = conn
                    .execute_command(&mut redis::cmd("LLEN").arg(self.base.get_full_key()))
                    .await?;

                if current_len > max_size as i32 {
                    conn.execute_command::<()>(&mut redis::cmd("LTRIM")
                        .arg(self.base.get_full_key())
                        .arg(0)
                        .arg(max_size as i64 - 1))
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Asynchronous streaming: Adding multiple elements
    pub async fn stream_add<'a, I>(&self, values: I, to_front: bool) -> RedissonResult<usize>
    where
        I: IntoIterator<Item = &'a V>,
    {
        let mut count = 0;
        for value in values {
            if to_front {
                self.add_first(value).await?;
            } else {
                self.add_last(value).await?;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Asynchronous streaming operations: Filter and add elements
    pub async fn stream_filter_add<F>(&self, values: &[V], filter: F, to_front: bool) -> RedissonResult<usize>
    where
        F: Fn(&V) -> bool,
    {
        let mut count = 0;
        for value in values {
            if filter(value) {
                if to_front {
                    self.add_first(value).await?;
                } else {
                    self.add_last(value).await?;
                }
                count += 1;
            }
        }
        Ok(count)
    }
}

/// Asynchronous list iterators
pub struct AsyncRListIterator<'a, V> {
    list: &'a AsyncRList<V>,
    current_index: i64,
}

impl<'a, V> AsyncRListIterator<'a, V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Fetch the next element asynchronously
    pub async fn next(&mut self) -> RedissonResult<Option<V>> {
        match self.list.get(self.current_index).await {
            Ok(Some(value)) => {
                self.current_index += 1;
                Ok(Some(value))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Getting the current location
    pub fn current_index(&self) -> i64 {
        self.current_index
    }

    /// Asynchronously check if there is a next element
    pub async fn has_next(&self) -> RedissonResult<bool> {
        Ok(self.list.size().await? > self.current_index as usize)
    }

    /// Get all the remaining elements asynchronously
    pub async fn collect(&mut self) -> RedissonResult<Vec<V>> {
        let size = self.list.size().await?;
        if self.current_index as usize >= size {
            return Ok(Vec::new());
        }

        self.list.range(self.current_index, -1).await
    }
}

// ============ Implementing common traits ============

impl<V> Clone for AsyncRList<V> {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            _marker: std::marker::PhantomData,
            trim_enabled: self.trim_enabled,
            trim_size: self.trim_size,
        }
    }
}


#[async_trait]
impl<V> AsyncRObject for AsyncRList<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
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

    async fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        self.base.get_expire_time().await
    }

    async fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        self.base.expire(duration).await
    }

    async fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        self.base.expire_at(timestamp).await
    }

    async fn clear_expire(&self) -> RedissonResult<bool> {
        self.base.clear_expire().await
    }
}


#[async_trait]
impl<V> AsyncRLockable for AsyncRList<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn get_lock(&self) -> AsyncRLock {
        AsyncRLock::new(
            self.base.connection_manager(),
            format!("{}:lock", self.base.get_full_key()),
            Duration::from_secs(30)
        )
    }

    fn get_fair_lock(&self) -> AsyncRFairLock {
        AsyncRFairLock::new(
            self.base.connection_manager(),
            format!("{}:fair_lock", self.base.get_full_key()),
            Duration::from_secs(30)
        )
    }

    async fn lock(&self) -> RedissonResult<()> {
        self.get_lock().lock().await
    }

    async fn try_lock(&self) -> RedissonResult<bool> {
        self.get_lock().try_lock().await
    }

    async fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        self.get_lock().try_lock_with_timeout(wait_time).await
    }

    async fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()> {
        self.get_lock().lock_with_lease_time(lease_time).await
    }

    async fn unlock(&self) -> RedissonResult<bool> {
        self.get_lock().unlock().await
    }

    async fn force_unlock(&self) -> RedissonResult<bool> {
        self.get_lock().force_unlock().await
    }

    async fn is_locked(&self) -> RedissonResult<bool> {
        self.get_lock().is_locked().await
    }

    async fn is_held_by_current_thread(&self) -> bool {
        self.get_lock().is_held_by_current_thread().await
    }
}