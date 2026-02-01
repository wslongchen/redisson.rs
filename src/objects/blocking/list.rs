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
use std::num::NonZero;
use std::sync::Arc;
use std::time::Duration;
use redis::Commands;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::{BaseDistributedObject, RFairLock, RLock, RLockable, RObject, RObjectBase, RedisConnection, RedissonResult, SyncRedisConnectionManager};

/// === RList ===
pub struct RList<V> {
    base: BaseDistributedObject,
    _marker: std::marker::PhantomData<V>,
    // List-specific Configuration
    trim_enabled: bool,
    trim_size: Option<usize>,
}

impl<V> RList<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(connection_manager: Arc<SyncRedisConnectionManager>, name: String) -> Self {
        Self {
            base: BaseDistributedObject::new(connection_manager, name),
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

    /// Adds element to end of list (same as add_last)
    pub fn add(&self, value: &V) -> RedissonResult<bool> {
        self.add_last(value)
    }

    /// Adds an element to the head of the list
    pub fn add_first(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let value_json = BaseDistributedObject::serialize(value)?;

        let len: i32 = conn.lpush(self.base.get_full_key(), value_json)?;
        self.maybe_trim(&mut conn)?;
        Ok(len > 0)
    }

    /// Adds the element to the end of the list
    pub fn add_last(&self, value: &V) -> RedissonResult<bool> {
        let mut conn = self.base.get_connection()?;
        let value_json = BaseDistributedObject::serialize(value)?;

        let len: i32 = conn.rpush(self.base.get_full_key(), value_json)?;
        self.maybe_trim(&mut conn)?;
        Ok(len > 0)
    }

    /// Inserts before the specified element
    pub fn add_before(&self, pivot: &V, value: &V) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection()?;
        let pivot_json = BaseDistributedObject::serialize(pivot)?;
        let value_json = BaseDistributedObject::serialize(value)?;

        let len: i32 = conn.linsert_before(self.base.get_full_key(), &pivot_json, value_json)?;
        self.maybe_trim(&mut conn)?;
        Ok(len as i64)
    }

    /// Inserts after the specified element
    pub fn add_after(&self, pivot: &V, value: &V) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection()?;
        let pivot_json = BaseDistributedObject::serialize(pivot)?;
        let value_json = BaseDistributedObject::serialize(value)?;

        let len: i32 = conn.linsert_after(self.base.get_full_key(), &pivot_json, value_json)?;
        self.maybe_trim(&mut conn)?;
        Ok(len as i64)
    }

    /// Batch adds to the head of the list
    pub fn add_all_first(&self, values: &[V]) -> RedissonResult<usize> {
        if values.is_empty() {
            return Ok(0);
        }

        let mut conn = self.base.get_connection()?;
        let values_json: Vec<String> = values.iter()
            .map(|v| BaseDistributedObject::serialize(v))
            .collect::<Result<_, _>>()?;

        let len: i32 = conn.lpush(self.base.get_full_key(), values_json)?;
        self.maybe_trim(&mut conn)?;
        Ok(len as usize)
    }

    /// Batch adds to the end of the list
    pub fn add_all_last(&self, values: &[V]) -> RedissonResult<usize> {
        if values.is_empty() {
            return Ok(0);
        }

        let mut conn = self.base.get_connection()?;
        let values_json: Vec<String> = values.iter()
            .map(|v| BaseDistributedObject::serialize(v))
            .collect::<Result<_, _>>()?;

        let len: i32 = conn.rpush(self.base.get_full_key(), values_json)?;
        self.maybe_trim(&mut conn)?;
        Ok(len as usize)
    }

    /// Batch add (automatically select location)
    pub fn add_all(&self, values: &[V], to_front: bool) -> RedissonResult<usize> {
        if to_front {
            self.add_all_first(values)
        } else {
            self.add_all_last(values)
        }
    }

    // ============ Remove operation ============

    /// Remove items from the head of a list (pop_front)
    pub fn pop_front(&self) -> RedissonResult<Option<V>> {
        self.pop_first()
    }

    /// Removes an element from the head of a list
    pub fn pop_first(&self) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;
        let result: Option<String> = conn.lpop(self.base.get_full_key(), None)?;

        match result {
            Some(value_json) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Removes multiple items from the head of a list
    pub fn pop_first_n(&self, count: usize) -> RedissonResult<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.base.get_connection()?;
        let results: Vec<String> = conn.lpop(self.base.get_full_key(), NonZero::new(count))?;

        let mut values = Vec::with_capacity(results.len());
        for value_json in results {
            let value: V = BaseDistributedObject::deserialize(&value_json)?;
            values.push(value);
        }

        Ok(values)
    }

    /// Removes an element from the end of a list
    pub fn pop_last(&self) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;
        let result: Option<String> = conn.rpop(self.base.get_full_key(), None)?;

        match result {
            Some(value_json) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Removes multiple items from the end of a list
    pub fn pop_last_n(&self, count: usize) -> RedissonResult<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.base.get_connection()?;
        let results: Vec<String> = conn.rpop(self.base.get_full_key(), NonZero::new(count))?;

        let mut values = Vec::with_capacity(results.len());
        for value_json in results {
            let value: V = BaseDistributedObject::deserialize(&value_json)?;
            values.push(value);
        }

        Ok(values)
    }

    /// Removes the specified element (by default, the first match is removed)
    pub fn remove(&self, value: &V) -> RedissonResult<bool> {
        self.remove_count(value, 1).map(|removed| removed > 0)
    }

    /// Removes the specified element a specified number of times
    pub fn remove_count(&self, value: &V, count: i64) -> RedissonResult<i64> {
        let mut conn = self.base.get_connection()?;
        let value_json = BaseDistributedObject::serialize(value)?;

        let removed: i64 = conn.lrem(self.base.get_full_key(), count as isize, value_json)?;
        Ok(removed)
    }

    /// Remove all matches
    pub fn remove_all(&self, value: &V) -> RedissonResult<i64> {
        self.remove_count(value, 0) // count为0表示移除所有
    }

    /// Removes an element by index
    pub fn remove_by_index(&self, index: i64) -> RedissonResult<Option<V>> {
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

        let mut conn = self.base.get_connection()?;
        let result: Option<String> = script
            .key(self.base.get_full_key())
            .arg(index)
            .invoke(&mut conn)?;

        match result {
            Some(value_json) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Removes elements by range
    pub fn remove_range(&self, start: i64, end: i64) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection()?;

        // We start by getting the elements in the range
        let values_json: Vec<String> = conn.lrange(self.base.get_full_key(), start as isize, end as isize)?;

        if values_json.is_empty() {
            return Ok(Vec::new());
        }

        // DESERIALIZE
        let mut values = Vec::with_capacity(values_json.len());
        for value_json in &values_json {
            let value: V = BaseDistributedObject::deserialize(value_json)?;
            values.push(value);
        }

        // Perform LTRIM to remove the range
        // Note: LTRIM keeps the specified range, so we need to remove the rest
        if start == 0 {
            // Remove the section from head to end
            let new_start = end + 1;
            let _: () = conn.ltrim(self.base.get_full_key(), new_start as isize, -1)?;
        } else if end == -1 {
            // Remove the part from start to end
            let _: () = conn.ltrim(self.base.get_full_key(), 0, (start - 1) as isize)?;
        } else {
            // Complex case: We need to remove the middle part
            let script = redis::Script::new(r#"
                local key = KEYS[1]
                local start = tonumber(ARGV[1])
                local end = tonumber(ARGV[2])
                
                -- Save the front and back
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
                
                return #left_part + #right_part
            "#);

            let _: i32 = script
                .key(self.base.get_full_key())
                .arg(start)
                .arg(end)
                .invoke(&mut conn)?;
        }

        Ok(values)
    }

    /// Removes and returns the first element (blocking version)
    pub fn bpop_first(&self, timeout: Duration) -> RedissonResult<Option<(String, V)>> {
        let mut conn = self.base.get_connection()?;
        let result: Option<(String, String)> = conn.blpop(self.base.get_full_key(), timeout.as_secs_f64())?;

        match result {
            Some((_key, value_json)) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some((self.base.get_full_key(), value)))
            }
            None => Ok(None),
        }
    }

    /// Removes and returns the last element (blocking version)
    pub fn bpop_last(&self, timeout: Duration) -> RedissonResult<Option<(String, V)>> {
        let mut conn = self.base.get_connection()?;
        let result: Option<(String, String)> = conn.brpop(self.base.get_full_key(), timeout.as_secs_f64())?;

        match result {
            Some((_key, value_json)) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some((self.base.get_full_key(), value)))
            }
            None => Ok(None),
        }
    }

    /// Popping from one list and pushing to another
    pub fn pop_and_push(&self, other_list_key: &str, from_right: bool) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;

        let result: Option<String> = if from_right {
            conn.rpoplpush(self.base.get_full_key(), other_list_key)?
        } else {
            conn.rpoplpush(self.base.get_full_key(), other_list_key)?
        };

        match result {
            Some(value_json) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Blocking version: Popping from one list and pushing into another
    pub fn bpop_and_push(&self, other_list_key: &str, timeout: Duration, from_right: bool) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;

        let result: Option<String> = if from_right {
            conn.brpoplpush(self.base.get_full_key(), other_list_key, timeout.as_secs_f64())?
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

            script
                .key(self.base.get_full_key())
                .key(other_list_key)
                .arg(timeout.as_secs() as usize)
                .invoke(&mut conn)?
        };

        match result {
            Some(value_json) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    // ============ Query operations ============

    /// Get elements by index
    pub fn get(&self, index: i64) -> RedissonResult<Option<V>> {
        let mut conn = self.base.get_connection()?;
        let result: Option<String> = conn.lindex(self.base.get_full_key(), index as isize)?;

        match result {
            Some(value_json) => {
                let value: V = BaseDistributedObject::deserialize(&value_json)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Sets the element by index
    pub fn set(&self, index: i64, value: &V) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        let value_json = BaseDistributedObject::serialize(value)?;

        conn.lset::<_, _, ()>(self.base.get_full_key(), index as isize, value_json)?;
        Ok(())
    }

    /// Getting the list size
    pub fn size(&self) -> RedissonResult<usize> {
        let mut conn = self.base.get_connection()?;
        let len: i32 = conn.llen(self.base.get_full_key())?;
        Ok(len as usize)
    }

    /// Check that the list is empty
    pub fn is_empty(&self) -> RedissonResult<bool> {
        Ok(self.size()? == 0)
    }

    /// Checks whether the specified element is included
    pub fn contains(&self, value: &V) -> RedissonResult<bool> {
        let values = self.range(0, -1)?;
        let target_json = BaseDistributedObject::serialize(value)?;

        for value in values {
            let value_json = BaseDistributedObject::serialize(&value)?;
            if value_json == target_json {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get the index of the element (first match)
    pub fn index_of(&self, value: &V) -> RedissonResult<Option<i64>> {
        self.index_of_from(value, 0)
    }

    /// Finds the element index at the specified position
    pub fn index_of_from(&self, value: &V, start_index: i64) -> RedissonResult<Option<i64>> {
        let target_json = BaseDistributedObject::serialize(value)?;
        let values = self.range(start_index, -1)?;

        for (i, value) in values.iter().enumerate() {
            let value_json = BaseDistributedObject::serialize(value)?;
            if value_json == target_json {
                return Ok(Some(start_index + i as i64));
            }
        }

        Ok(None)
    }

    /// Get the index of the last match
    pub fn last_index_of(&self, value: &V) -> RedissonResult<Option<i64>> {
        let target_json = BaseDistributedObject::serialize(value)?;
        let values = self.range(0, -1)?;

        for (i, value) in values.iter().enumerate().rev() {
            let value_json = BaseDistributedObject::serialize(value)?;
            if value_json == target_json {
                return Ok(Some(i as i64));
            }
        }

        Ok(None)
    }

    /// Getting a range element
    pub fn range(&self, start: i64, end: i64) -> RedissonResult<Vec<V>> {
        let mut conn = self.base.get_connection()?;
        let values_json: Vec<String> = conn.lrange(self.base.get_full_key(), start as isize, end as isize)?;

        let mut values = Vec::with_capacity(values_json.len());
        for value_json in values_json {
            let value: V = BaseDistributedObject::deserialize(&value_json)?;
            values.push(value);
        }

        Ok(values)
    }

    /// Getting all elements
    pub fn all(&self) -> RedissonResult<Vec<V>> {
        self.range(0, -1)
    }

    /// Get the first N elements
    pub fn first_n(&self, n: usize) -> RedissonResult<Vec<V>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        self.range(0, (n - 1) as i64)
    }

    /// Get the last N elements
    pub fn last_n(&self, n: usize) -> RedissonResult<Vec<V>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let size = self.size()?;
        if n >= size {
            return self.all();
        }
        self.range((size - n) as i64, -1)
    }

    /// Get the first element
    pub fn first(&self) -> RedissonResult<Option<V>> {
        self.get(0)
    }

    /// Get the last element
    pub fn last(&self) -> RedissonResult<Option<V>> {
        self.get(-1)
    }

    /// Iterator traversal
    pub fn iter(&self) -> RListIterator<'_, V> {
        RListIterator {
            list: self,
            current_index: 0,
        }
    }

    // ============ Modify operations ============

    /// Pruning the list
    pub fn trim(&self, start: i64, end: i64) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        conn.ltrim::<_, ()>(self.base.get_full_key(), start as isize, end as isize)?;
        Ok(())
    }

    /// Clearing a list
    pub fn clear(&self) -> RedissonResult<()> {
        let mut conn = self.base.get_connection()?;
        conn.del::<_, ()>(self.base.get_full_key())?;
        Ok(())
    }

    /// Keep the first N elements
    pub fn keep_first(&self, n: usize) -> RedissonResult<()> {
        if n == 0 {
            self.clear()
        } else {
            self.trim(0, (n - 1) as i64)
        }
    }

    /// Keep the last N elements
    pub fn keep_last(&self, n: usize) -> RedissonResult<()> {
        let size = self.size()?;
        if n == 0 {
            self.clear()
        } else if n >= size {
            Ok(())
        } else {
            self.trim((size - n) as i64, -1)
        }
    }

    /// Remove the first N elements
    pub fn remove_first_n(&self, n: usize) -> RedissonResult<Vec<V>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        self.remove_range(0, (n - 1) as i64)
    }

    /// Remove the last N elements
    pub fn remove_last_n(&self, n: usize) -> RedissonResult<Vec<V>> {
        let size = self.size()?;
        if n == 0 {
            return Ok(Vec::new());
        }
        if n >= size {
            return self.all();
        }
        self.remove_range((size - n) as i64, -1)
    }

    /// Quick setup (no return value)
    pub fn fast_set(&self, index: i64, value: &V) -> RedissonResult<()> {
        self.set(index, value)
    }

    /// Fast removal (no return value)
    pub fn fast_remove(&self, value: &V) -> RedissonResult<bool> {
        self.remove(value)
    }

    // ============ Utility methods ============

    /// Convert the list to Vec
    pub fn to_vec(&self) -> RedissonResult<Vec<V>> {
        self.all()
    }

    /// Create a sublist (without modifying the original)
    pub fn sub_list(&self, start: i64, end: i64) -> RedissonResult<Vec<V>> {
        self.range(start, end)
    }

    /// Checks whether the list contains all the specified elements
    pub fn contains_all(&self, values: &[V]) -> RedissonResult<bool> {
        let list_values = self.all()?;

        for target in values {
            let target_json = BaseDistributedObject::serialize(target)?;
            let mut found = false;

            for value in &list_values {
                let value_json = BaseDistributedObject::serialize(value)?;
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

    /// Checks whether a list contains any of the specified elements
    pub fn contains_any(&self, values: &[V]) -> RedissonResult<bool> {
        let list_values = self.all()?;

        for target in values {
            let target_json = BaseDistributedObject::serialize(target)?;

            for value in &list_values {
                let value_json = BaseDistributedObject::serialize(value)?;
                if value_json == target_json {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Add an element if it doesn't exist
    pub fn add_if_absent(&self, value: &V) -> RedissonResult<bool> {
        if self.contains(value)? {
            Ok(false)
        } else {
            self.add_last(value)
        }
    }

    /// Batch add elements if they don't exist
    pub fn add_all_if_absent(&self, values: &[V]) -> RedissonResult<usize> {
        let mut added = 0;

        for value in values {
            if !self.contains(value)? {
                self.add_last(value)?;
                added += 1;
            }
        }

        Ok(added)
    }

    fn maybe_trim(&self, conn: &mut RedisConnection) -> RedissonResult<()> {
        if self.trim_enabled {
            if let Some(max_size) = self.trim_size {
                let current_len: i32 = conn.llen(self.base.get_full_key())?;
                if current_len > max_size as i32 {
                    let start = 0;
                    let end = max_size as i64 - 1;
                    conn.ltrim::<_, ()>(self.base.get_full_key(), start, end as isize)?;
                }
            }
        }
        Ok(())
    }

    /// Asynchronous streaming: Adding multiple elements
    pub fn stream_add<'a, I>(&self, values: I, to_front: bool) -> RedissonResult<usize>
    where
        I: IntoIterator<Item = &'a V>,
    {
        let mut count = 0;
        for value in values {
            if to_front {
                self.add_first(value)?;
            } else {
                self.add_last(value)?;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Asynchronous streaming operations: Filter and add elements
    pub fn stream_filter_add<F>(&self, values: &[V], filter: F, to_front: bool) -> RedissonResult<usize>
    where
        F: Fn(&V) -> bool,
    {
        let mut count = 0;
        for value in values {
            if filter(value) {
                if to_front {
                    self.add_first(value)?;
                } else {
                    self.add_last(value)?;
                }
                count += 1;
            }
        }
        Ok(count)
    }
}

/// List iterators
pub struct RListIterator<'a, V> {
    list: &'a RList<V>,
    current_index: i64,
}

impl<'a, V> Iterator for RListIterator<'a, V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Item = RedissonResult<V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.list.get(self.current_index) {
            Ok(Some(value)) => {
                self.current_index += 1;
                Some(Ok(value))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a, V> RListIterator<'a, V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Getting the current location
    pub fn current_index(&self) -> i64 {
        self.current_index
    }

    /// Whether there is a next element
    pub fn has_next(&self) -> RedissonResult<bool> {
        Ok(self.list.size()? > self.current_index as usize)
    }
}

// ============ Implementing common traits ============

impl<V> Clone for RList<V> {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            _marker: std::marker::PhantomData,
            trim_enabled: self.trim_enabled,
            trim_size: self.trim_size,
        }
    }
}

// RList lock trait implementation (by inheriting from BaseDistributedObject)
impl<V> RObject for RList<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn get_name(&self) -> &str {
        self.base.get_name()
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

    fn get_expire_time(&self) -> RedissonResult<Option<Duration>> {
        self.base.get_expire_time()
    }

    fn expire(&self, duration: Duration) -> RedissonResult<bool> {
        self.base.expire(duration)
    }

    fn expire_at(&self, timestamp: i64) -> RedissonResult<bool> {
        self.base.expire_at(timestamp)
    }

    fn clear_expire(&self) -> RedissonResult<bool> {
        self.base.clear_expire()
    }
}

impl<V> RLockable for RList<V>
where
    V: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn get_lock(&self) -> RLock {
        self.base.get_lock()
    }

    fn get_fair_lock(&self) -> RFairLock {
        self.base.get_fair_lock()
    }

    fn lock(&self) -> RedissonResult<()> {
        self.base.lock()
    }

    fn try_lock(&self) -> RedissonResult<bool> {
        self.base.try_lock()
    }

    fn try_lock_timeout(&self, wait_time: Duration) -> RedissonResult<bool> {
        self.base.try_lock_timeout(wait_time)
    }

    fn lock_lease(&self, lease_time: Duration) -> RedissonResult<()> {
        self.base.lock_lease(lease_time)
    }

    fn unlock(&self) -> RedissonResult<bool> {
        self.base.unlock()
    }

    fn force_unlock(&self) -> RedissonResult<bool> {
        self.base.force_unlock()
    }

    fn is_locked(&self) -> RedissonResult<bool> {
        self.base.is_locked()
    }

    fn is_held_by_current_thread(&self) -> bool {
        self.base.is_held_by_current_thread()
    }
}
