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

use once_cell::sync::Lazy;
use redis::Script;

pub static LOCK_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local ttl = ARGV[2]
        
        -- Check if the lock exists
        if redis.call('exists', key) == 0 then
            -- Lock does not exist, create lock
            redis.call('hset', key, thread_id, 1)
            redis.call('pexpire', key, ttl)
            return 1  -- Lock acquisition successfully
        end
        
        -- Check if it is a lock for the current thread (reentry)
        if redis.call('hexists', key, thread_id) == 1 then
            -- Reentrant count +1
            local new_count = redis.call('hincrby', key, thread_id, 1)
            redis.call('pexpire', key, ttl)
            return new_count  -- Returns the new reentrant count
        end
        
        return 0  -- The lock is held by another thread
    "#)
});

pub static UNLOCK_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local ttl = ARGV[2]
        
        -- Checks whether the thread holds the lock
        if redis.call('hexists', key, thread_id) == 0 then
            return -1  -- The thread does not hold the lock
        end
        
        -- Decrease the reentrant count
        local counter = redis.call('hincrby', key, thread_id, -1)
        
        if counter > 0 then
            -- There are also reentrant counts, and updated expiration times
            redis.call('pexpire', key, ttl)
            return counter  -- Returns the remaining reentrant count
        else
            -- Release the lock completely, removing the thread's entry
            redis.call('hdel', key, thread_id)
            
            -- Check if the hash table is empty
            local remaining = redis.call('hlen', key)
            if remaining == 0 then
                -- Removing an entire lock
                redis.call('del', key)
                return 0  -- Lock fully released
            else
                -- There are other threads that hold the lock and only update the expiration time
                redis.call('pexpire', key, ttl)
                return 0  -- The lock for the current thread has been released
            end
        end
    "#)
});

/// Renewal script 
pub static RENEW_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local new_ttl = ARGV[2]
        
        -- Checks whether the thread holds the lock
        if redis.call('hexists', key, thread_id) == 1 then
            redis.call('pexpire', key, new_ttl)
            return 1  -- Successful renewal
        end
        
        return 0  -- The renewal failed and the thread did not hold the lock
    "#)
});

/// Force the unlock script 
pub static FORCE_UNLOCK_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        
        -- Get lock information first (for logging or auditing)
        local lock_info = redis.call('hgetall', key)
        
        -- Force lock removal
        local result = redis.call('del', key)
        
        -- Returns the removed lock information and the result
        return {result, #lock_info / 2}  -- Returns the removal result and the number of threads held
    "#)
});

/// Attempt to acquire lock script (with timeout)
pub static TRY_LOCK_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local ttl = ARGV[2]
        
        -- Attempt to acquire lock (non-blocking)
        if redis.call('exists', key) == 0 then
            redis.call('hset', key, thread_id, 1)
            redis.call('pexpire', key, ttl)
            return 1  -- Lock acquisition successfully
        end
        
        -- Check if it is already the holder (reentry)
        if redis.call('hexists', key, thread_id) == 1 then
            local new_count = redis.call('hincrby', key, thread_id, 1)
            redis.call('pexpire', key, ttl)
            return new_count  -- Successful reentry
        end
        
        return 0  -- The lock is held by another thread
    "#)
});

/// The lock status check script 
pub static LOCK_STATUS_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        
        -- Check if the lock exists
        if redis.call('exists', key) == 1 then
            -- Gets the lock count for the current thread
            local count = redis.call('hget', key, thread_id)
            -- Retrieve the TTL of the lock
            local ttl = redis.call('pttl', key)
            -- Gets the total number of holders
            local holders = redis.call('hlen', key)
            
            return {count or 0, ttl, holders}
        end
        
        return {0, -2, 0}  -- Lock does not exist
    "#)
});

/// Read the lock acquisition script 
pub static READ_LOCK_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local ttl = ARGV[2]
        
        -- Retrieve the current locking mode
        local mode = redis.call('hget', key, 'mode')
        
        if not mode then
            -- Creating read locks
            redis.call('hset', key, 'mode', 'read', thread_id, 1)
            redis.call('pexpire', key, ttl)
            return 1  -- The first reader
        end
        
        if mode == 'read' then
            -- Increase the reader count
            local count = redis.call('hincrby', key, thread_id, 1)
            redis.call('pexpire', key, ttl)
            return count  -- Returns the reader count.
        end
        
        -- A write lock exists and a read lock cannot be acquired
        return 0
    "#)
});

/// Write lock acquisition script 
pub static WRITE_LOCK_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local ttl = ARGV[2]
        
        -- Retrieve the current locking mode
        local mode = redis.call('hget', key, 'mode')
        
        if not mode then
            -- Creating a write lock
            redis.call('hset', key, 'mode', 'write', thread_id, 1)
            redis.call('pexpire', key, ttl)
            return 1  -- The write lock was acquired successfully
        end
        
        if mode == 'write' then
            -- Check if it is already the holder
            local count = redis.call('hget', key, thread_id)
            if count then
                -- A reentry write lock
                local new_count = redis.call('hincrby', key, thread_id, 1)
                redis.call('pexpire', key, ttl)
                return new_count
            end
        end
        
        -- There are read locks or write locks for other threads
        return 0
    "#)
});

/// Read the lock release script 
pub static READ_UNLOCK_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local ttl = ARGV[2]
        
        -- Obtaining the lock pattern
        local mode = redis.call('hget', key, 'mode')
        
        if not mode then
            return 2  -- The lock no longer exists
        end
        
        if mode == 'read' then
            -- Getting the reader count
            local count = redis.call('hget', key, thread_id)
            if not count then
                return -1  -- The thread does not hold the read lock
            end
            
            -- Decrement count
            local new_count = redis.call('hincrby', key, thread_id, -1)
            
            if new_count <= 0 then
                -- Delete the reader
                redis.call('hdel', key, thread_id)
                
                -- Check if there are still readers
                local readers = 0
                local fields = redis.call('hkeys', key)
                for _, field in ipairs(fields) do
                    if field ~= 'mode' then
                        readers = readers + 1
                    end
                end
                
                if readers == 0 then
                    -- Removing an entire lock
                    redis.call('del', key)
                    return 1  -- Full release
                end
            end
            
            -- Update expiration time
            redis.call('pexpire', key, ttl)
            return 1  -- Release success
        end
        
        return 0  -- Not in read-lock mode
    "#)
});

/// Write the lock release script 
pub static WRITE_UNLOCK_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local ttl = ARGV[2]
        
        -- Obtaining the lock pattern
        local mode = redis.call('hget', key, 'mode')
        
        if not mode then
            return 2  -- The lock no longer exists
        end
        
        if mode == 'write' then
            -- Get the write lock count
            local count = redis.call('hget', key, thread_id)
            if not count then
                return -1  -- The thread does not hold the write lock
            end
            
            -- Decrement count
            local new_count = redis.call('hincrby', key, thread_id, -1)
            
            if new_count <= 0 then
                -- Release the write lock completely
                redis.call('hdel', key, thread_id, 'mode')
                
                -- Check if there are other fields
                if redis.call('hlen', key) == 0 then
                    redis.call('del', key)
                end
                return 1  -- Full release
            end
            
            -- Update expiration time
            redis.call('pexpire', key, ttl)
            return 1  -- Release successful (and reentrant count)
        end
        
        return 0  -- Not write lock mode
    "#)
});

/// Read the lock renewal script 
pub static READ_RENEW_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local new_ttl = ARGV[2]
        
        -- Checking lock patterns
        local mode = redis.call('hget', key, 'mode')
        
        if mode == 'read' then
            -- Check if the read lock is held
            local count = redis.call('hget', key, thread_id)
            if count then
                redis.call('pexpire', key, new_ttl)
                return 1  -- Successful renewal
            end
        end
        
        return 0  -- Renewal failed
    "#)
});

/// Write the lock renewal script 
pub static WRITE_RENEW_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local thread_id = ARGV[1]
        local new_ttl = ARGV[2]
        
        -- Checking lock patterns
        local mode = redis.call('hget', key, 'mode')
        
        if mode == 'write' then
            -- Checks whether the write lock is held
            local count = redis.call('hget', key, thread_id)
            if count then
                redis.call('pexpire', key, new_ttl)
                return 1  -- Successful renewal
            end
        end
        
        return 0  -- Renewal failed
    "#)
});

/// Get the reader count script 
pub static GET_READER_COUNT_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        
        -- Obtaining the lock pattern
        local mode = redis.call('hget', key, 'mode')
        
        if not mode or mode ~= 'read' then
            return 0  -- Not in read-lock mode
        end
        
        -- Count the total number of readers (including reentry count)
        local total = 0
        local all_fields = redis.call('hgetall', key)
        
        for i = 1, #all_fields, 2 do
            local field = all_fields[i]
            if field ~= 'mode' then
                total = total + tonumber(all_fields[i + 1])
            end
        end
        
        return total
    "#)
});

/// Publish/subscribe scripts
pub static PUBLISH_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local channel = KEYS[1]
        local message = ARGV[1]
        local persist = ARGV[2] or 'false'
        
        -- Release a message
        local receivers = redis.call('publish', channel, message)
        
        -- If persistence is required, save to history
        if persist == 'true' then
            local history_key = channel .. ':history'
            local timestamp = redis.call('time')[1]  -- Getting the current time
            
            -- Save timestamped messages
            local message_with_time = timestamp .. '|' .. message
            redis.call('lpush', history_key, message_with_time)
            
            -- Limit history size (Max 100)
            redis.call('ltrim', history_key, 0, 99)
            
            -- Set expiration time (7 days)
            redis.call('expire', history_key, 604800)
        end
        
        -- Updating statistics
        redis.call('hincrby', channel .. ':stats', 'messages_published', 1)
        redis.call('expire', channel .. ':stats', 3600)  -- 1小时过期
        
        return receivers
    "#)
});

/// Counter script (CountDownLatch)
pub static COUNTDOWN_LATCH_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local latch_key = KEYS[1]
        local count_key = latch_key .. ':count'
        local waiters_key = latch_key .. ':waiters'
        local stats_key = latch_key .. ':stats'
        
        local command = ARGV[1]
        local value = tonumber(ARGV[2] or 0)
        
        -- Initializing statistics
        if redis.call('exists', stats_key) == 0 then
            redis.call('hmset', stats_key, 
                'created', redis.call('time')[1],
                'countdowns', 0,
                'awaits', 0
            )
        end
        
        if command == 'initialize' then
            -- Initialize the counter
            if redis.call('exists', count_key) == 0 then
                redis.call('set', count_key, value)
                redis.call('expire', count_key, 86400)  -- 24小时
                return 1
            end
            return 0
            
        elseif command == 'countDown' then
            -- Decrement count
            local remaining = redis.call('decr', count_key)
            
            -- Update statistics
            redis.call('hincrby', stats_key, 'countdowns', 1)
            
            if remaining <= 0 then
                -- The counter is 0 and all waiters are notified
                local waiters = redis.call('smembers', waiters_key)
                local notified = 0
                
                for _, channel in ipairs(waiters) do
                    redis.call('publish', channel, 'ready')
                    notified = notified + 1
                end
                
                -- Clean up
                redis.call('del', waiters_key)
                
                -- Record completion time
                redis.call('hset', stats_key, 'completed', redis.call('time')[1])
                
                return {0, notified}  -- Returns the remaining count and the number of waiters for the notification
            end
            
            return {remaining, 0}
            
        elseif command == 'await' then
            -- Wait for the counter
            local channel = ARGV[3]
            local timeout = tonumber(ARGV[4] or 0)
            
            -- Update statistics
            redis.call('hincrby', stats_key, 'awaits', 1)
            
            local count = tonumber(redis.call('get', count_key) or 0)
            
            if count <= 0 then
                return {1, 0}  -- It's done
            end
            
            -- Registered waiters
            redis.call('sadd', waiters_key, channel)
            if timeout > 0 then
                redis.call('expire', waiters_key, timeout)
            end
            
            return {0, count}  -- Returns the waiting status and the current count
            
        elseif command == 'getCount' then
            -- Getting the current count
            local count = tonumber(redis.call('get', count_key) or 0)
            return math.max(0, count)
            
        elseif command == 'reset' then
            -- Reset the counter
            redis.call('set', count_key, value)
            redis.call('del', waiters_key)
            return 1
            
        elseif command == 'getStats' then
            -- Getting statistics
            return redis.call('hgetall', stats_key)
        end
        
        return -1  -- Unknown command
    "#)
});

/// Semaphore scripts
pub static SEMAPHORE_ACQUIRE_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local semaphore_key = KEYS[1]
        local permit_id = ARGV[1]
        local permits_requested = tonumber(ARGV[2])
        local timeout = tonumber(ARGV[3])
        local current_time = tonumber(ARGV[4])
        
        -- Get the maximum number of licenses
        local max_permits = tonumber(redis.call('get', semaphore_key .. ':max') or 1)
        
        -- Clean up expired permissions
        redis.call('zremrangebyscore', semaphore_key, 0, current_time - timeout)
        
        -- Gets the current number of licenses
        local current_permits = redis.call('zcard', semaphore_key)
        
        -- Compute available licenses
        local available_permits = max_permits - current_permits
        
        if available_permits >= permits_requested then
            -- There's enough permission
            for i = 1, permits_requested do
                redis.call('zadd', semaphore_key, current_time, permit_id .. ':' .. i)
            end
            
            -- Update statistics
            redis.call('hincrby', semaphore_key .. ':stats', 'acquired', permits_requested)
            redis.call('expire', semaphore_key .. ':stats', 3600)
            
            return {1, available_permits - permits_requested}  -- On success, the remaining license is returned
        end
        
        -- Insufficient permission
        redis.call('hincrby', semaphore_key .. ':stats', 'rejected', permits_requested)
        
        return {0, available_permits}  -- Failure, return the available license
    "#)
});

/// Current limiter script
pub static RATE_LIMITER_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local limit_key = KEYS[1]
        local algorithm = ARGV[1]  -- 'token_bucket' or 'sliding_window'
        
        if algorithm == 'token_bucket' then
            -- The token bucket algorithm
            local tokens_key = limit_key .. ':tokens'
            local timestamp_key = limit_key .. ':timestamp'
            local rate = tonumber(ARGV[2])      -- Tokens per second
            local capacity = tonumber(ARGV[3])  -- Bucket capacity
            local requested = tonumber(ARGV[4]) -- Number of request tokens
            local now = tonumber(ARGV[5])       -- Current time (seconds)
            
            -- Gets the last update time
            local last_time = tonumber(redis.call('get', timestamp_key) or now)
            
            -- Compute the newly generated token
            local time_passed = now - last_time
            local new_tokens = math.floor(time_passed * rate)
            
            -- Gets the current number of tokens
            local current_tokens = tonumber(redis.call('get', tokens_key) or capacity)
            
            -- Update number of tokens (up to capacity)
            current_tokens = math.min(capacity, current_tokens + new_tokens)
            
            if current_tokens >= requested then
                -- There are enough tokens
                current_tokens = current_tokens - requested
                redis.call('set', tokens_key, current_tokens)
                redis.call('set', timestamp_key, now)
                
                -- Calculate the time until the next available token
                local wait_time = 0
                if current_tokens < requested then
                    wait_time = math.ceil((requested - current_tokens) / rate)
                end
                
                return {1, current_tokens, wait_time}  -- SUCCESS
            else
                -- Insufficient tokens
                local deficit = requested - current_tokens
                local wait_time = math.ceil(deficit / rate)
                return {0, current_tokens, wait_time}  -- FAILURE
            end
            
        elseif algorithm == 'sliding_window' then
            -- Sliding window algorithm
            local window_size = tonumber(ARGV[2])      -- Window size (seconds)
            local max_requests = tonumber(ARGV[3])     -- Maximum number of requests
            local request_id = ARGV[4]                 -- Request ID
            local current_time = tonumber(ARGV[5])     -- Current timestamp
            
            -- Clean up expired requests
            local cutoff_time = current_time - window_size
            redis.call('zremrangebyscore', limit_key, 0, cutoff_time)
            
            -- Gets the current number of requests
            local current_count = redis.call('zcard', limit_key)
            
            if current_count < max_requests then
                -- Allow requests
                redis.call('zadd', limit_key, current_time, request_id)
                redis.call('expire', limit_key, window_size * 2)
                
                local remaining = max_requests - current_count - 1
                local reset_time = current_time + window_size
                
                return {1, remaining, reset_time}  -- SUCCESS
            else
                -- Deny the request
                local oldest = redis.call('zrange', limit_key, 0, 0, 'withscores')
                local reset_time = tonumber(oldest[2]) + window_size
                
                return {0, 0, reset_time}  -- FAILURE
            end
        end
        
        return {-1, 0, 0}  -- Unknown algorithm
    "#)
});

/// Atomic operation script
pub static COMPARE_AND_SET_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        local operation = ARGV[1]  -- 'set', 'increment', 'decrement', 'append'
        
        if operation == 'set' then
            local expected = ARGV[2]
            local new_value = ARGV[3]
            local ttl = tonumber(ARGV[4] or 0)
            
            local current = redis.call('get', key)
            
            if (current == false and expected == '') or current == expected then
                redis.call('set', key, new_value)
                if ttl > 0 then
                    redis.call('expire', key, ttl)
                end
                return {1, new_value}  -- success
            end
            return {0, current}  -- failure
            
        elseif operation == 'increment' then
            local delta = tonumber(ARGV[2])
            local ttl = tonumber(ARGV[3] or 0)
            
            local new_value = redis.call('incrby', key, delta)
            if ttl > 0 then
                redis.call('expire', key, ttl)
            end
            return {1, new_value}  -- success
            
        elseif operation == 'decrement' then
            local delta = tonumber(ARGV[2])
            local ttl = tonumber(ARGV[3] or 0)
            
            local new_value = redis.call('decrby', key, delta)
            if ttl > 0 then
                redis.call('expire', key, ttl)
            end
            return {1, new_value}  -- success
            
        elseif operation == 'append' then
            local expected = ARGV[2]
            local append_value = ARGV[3]
            
            local current = redis.call('get', key)
            
            if (current == false and expected == '') or current == expected then
                local new_value = (current or '') .. append_value
                redis.call('set', key, new_value)
                return {1, new_value}  -- success
            end
            return {0, current}  -- failure
        end
        
        return {-1, nil}  -- Unknown operation
    "#)
});

/// Bloom filter script
pub static BLOOM_FILTER_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local filter_key = KEYS[1]
        local command = ARGV[1]
        
        if command == 'add' then
            -- Batch add
            local added = 0
            for i = 2, #ARGV do
                local item = ARGV[i]
                if redis.call('bf.add', filter_key, item) == 1 then
                    added = added + 1
                end
            end
            return added
            
        elseif command == 'exists' then
            -- Batch inspection
            local results = {}
            for i = 2, #ARGV do
                local item = ARGV[i]
                table.insert(results, redis.call('bf.exists', filter_key, item))
            end
            return results
            
        elseif command == 'reserve' then
            local error_rate = ARGV[2]
            local capacity = ARGV[3]
            
            if redis.call('exists', filter_key) == 0 then
                return redis.call('bf.reserve', filter_key, error_rate, capacity)
            end
            return 0
            
        elseif command == 'info' then
            -- Get bloom filter information
            if redis.call('exists', filter_key) == 1 then
                return redis.call('bf.info', filter_key)
            end
            return {}
            
        elseif command == 'stats' then
            -- Statistical information
            local stats_key = filter_key .. ':stats'
            local added = tonumber(redis.call('hget', stats_key, 'added') or 0)
            local checked = tonumber(redis.call('hget', stats_key, 'checked') or 0)
            local false_positives = tonumber(redis.call('hget', stats_key, 'false_positives') or 0)
            
            return {added, checked, false_positives}
        end
        
        return 0
    "#)
});

/// Geospatial scripts
pub static GEO_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local geo_key = KEYS[1]
        local command = ARGV[1]
        
        if command == 'add' then
            -- Batch add
            local added = 0
            local i = 2
            while i <= #ARGV do
                local member = ARGV[i]
                local longitude = tonumber(ARGV[i + 1])
                local latitude = tonumber(ARGV[i + 2])
                
                if redis.call('geoadd', geo_key, longitude, latitude, member) == 1 then
                    added = added + 1
                end
                i = i + 3
            end
            return added
            
        elseif command == 'dist' then
            local member1 = ARGV[2]
            local member2 = ARGV[3]
            local unit = ARGV[4] or 'm'
            
            return redis.call('geodist', geo_key, member1, member2, unit)
            
        elseif command == 'radius' then
            local longitude = tonumber(ARGV[2])
            local latitude = tonumber(ARGV[3])
            local radius = tonumber(ARGV[4])
            local unit = ARGV[5] or 'm'
            local options = ARGV[6] or ''
            
            return redis.call('georadius', geo_key, longitude, latitude, radius, unit, options)
            
        elseif command == 'radius_by_member' then
            local member = ARGV[2]
            local radius = tonumber(ARGV[3])
            local unit = ARGV[4] or 'm'
            local options = ARGV[5] or ''
            
            return redis.call('georadiusbymember', geo_key, member, radius, unit, options)
            
        elseif command == 'hash' then
            local member = ARGV[2]
            return redis.call('geohash', geo_key, member)
            
        elseif command == 'pos' then
            local member = ARGV[2]
            return redis.call('geopos', geo_key, member)
            
        elseif command == 'remove' then
            local removed = 0
            for i = 2, #ARGV do
                local member = ARGV[i]
                if redis.call('zrem', geo_key, member) == 1 then
                    removed = removed + 1
                end
            end
            return removed
        end
        
        return nil
    "#)
});

/// Get the lock holder list script
pub static GET_LOCK_HOLDERS_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local key = KEYS[1]
        
        -- Get all holders
        local holders = redis.call('hgetall', key)
        local result = {}
        
        for i = 1, #holders, 2 do
            local holder = holders[i]
            local count = holders[i + 1]
            
            -- Get the holder's lock information
            local holder_info = {
                thread_id = holder,
                count = tonumber(count),
                ttl = redis.call('pttl', key)
            }
            
            table.insert(result, holder_info)
        end
        
        return result
    "#)
});

/// Bulk lock operation script
pub static BATCH_LOCK_OPERATION_SCRIPT: Lazy<Script> = Lazy::new(|| {
    Script::new(r#"
        local results = {}
        
        for i = 1, #KEYS do
            local key = KEYS[i]
            local operation = ARGV[(i-1)*3 + 1]
            local thread_id = ARGV[(i-1)*3 + 2]
            local ttl = ARGV[(i-1)*3 + 3]
            
            if operation == 'lock' then
                if redis.call('exists', key) == 0 then
                    redis.call('hset', key, thread_id, 1)
                    redis.call('pexpire', key, ttl)
                    table.insert(results, 1)
                elseif redis.call('hexists', key, thread_id) == 1 then
                    local count = redis.call('hincrby', key, thread_id, 1)
                    redis.call('pexpire', key, ttl)
                    table.insert(results, count)
                else
                    table.insert(results, 0)
                end
                
            elseif operation == 'unlock' then
                if redis.call('hexists', key, thread_id) == 0 then
                    table.insert(results, -1)
                else
                    local counter = redis.call('hincrby', key, thread_id, -1)
                    if counter > 0 then
                        redis.call('pexpire', key, ttl)
                        table.insert(results, counter)
                    else
                        redis.call('hdel', key, thread_id)
                        if redis.call('hlen', key) == 0 then
                            redis.call('del', key)
                        else
                            redis.call('pexpire', key, ttl)
                        end
                        table.insert(results, 0)
                    end
                end
                
            elseif operation == 'status' then
                if redis.call('exists', key) == 1 then
                    local count = redis.call('hget', key, thread_id)
                    local ttl = redis.call('pttl', key)
                    table.insert(results, {count or 0, ttl})
                else
                    table.insert(results, {0, -2})
                end
            end
        end
        
        return results
    "#)
});
