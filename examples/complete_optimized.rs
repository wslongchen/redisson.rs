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

use std::time::Duration;
use std::thread;
use serde::{Serialize, Deserialize};
use tokio::runtime::Runtime;
use redisson::{AsyncRedissonClient, BatchResult, RLockable, RedissonClient, RedissonConfig, RedissonError, RedissonResult};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct User {
    id: u64,
    name: String,
    email: String,
    roles: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Product {
    id: String,
    name: String,
    price: f64,
    stock: i32,
    tags: Vec<String>,
}

fn main() -> RedissonResult<()> {
    // 1. Configure client
    let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
        .with_pool_size(20)
        .with_connection_timeout(Duration::from_secs(5))
        .with_response_timeout(Duration::from_secs(3))
        .with_lock_expire_time(Duration::from_secs(30))
        .with_watchdog_timeout(Duration::from_secs(10))
        .with_retry_count(3)
        .with_drift_factor(0.01);

    println!("🚀 Creating Redisson client...");

    // 2. Create synchronous client
    let client = RedissonClient::new(config)?;
    println!("✅ Client created successfully");

    // 3. Basic data structures example
    basic_data_structures(&client)?;

    // 4. Distributed locks example
    distributed_locks(&client)?;

    // 5. Advanced synchronizers example
    advanced_synchronizers(&client)?;

    // 6. Batch operations example
    batch_operations(&client)?;

    // 7. Transaction operations example
    transaction_operations(&client)?;

    // 8. Pub/Sub example
    pubsub_example(&client)?;

    // 9. Delayed queue example
    delayed_queue_example(&client)?;

    // 10. Asynchronous operations example
    async_example()?;

    println!("\n🎉 All examples executed successfully!");

    // Clean up resources
    client.shutdown()?;
    println!("🔌 Client shutdown complete");

    Ok(())
}

fn basic_data_structures(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n📦 Basic Data Structures Example:");

    // RBucket example
    println!("1. RBucket (Key-Value Pair):");
    let bucket = client.get_bucket::<User>("user:alice");

    let alice = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        roles: vec!["admin".to_string(), "user".to_string()],
    };

    bucket.set(&alice)?;
    println!("   ✅ Set user data");

    let retrieved: Option<User> = bucket.get()?;
    println!("   ✅ Retrieved user data: {:?}", retrieved.map(|u| u.name));

    // Set with TTL
    bucket.set_with_ttl(&alice, Duration::from_secs(60))?;
    println!("   ✅ Set with 60 seconds TTL");

    // RMap example
    println!("\n2. RMap (Hash Map):");
    let product_map = client.get_map::<String, Product>("products");

    let laptop = Product {
        id: "p001".to_string(),
        name: "Laptop".to_string(),
        price: 999.99,
        stock: 50,
        tags: vec!["electronics".to_string(), "computer".to_string()],
    };

    let phone = Product {
        id: "p002".to_string(),
        name: "Smartphone".to_string(),
        price: 699.99,
        stock: 100,
        tags: vec!["electronics".to_string(), "mobile".to_string()],
    };

    product_map.put(&"p001".to_string(), &laptop)?;
    product_map.put(&"p002".to_string(), &phone)?;
    println!("   ✅ Added 2 products");

    let laptop_retrieved = product_map.get(&"p001".to_string())?;
    println!("   ✅ Retrieved product p001: {:?}", laptop_retrieved.map(|p| p.name));

    let size = product_map.size()?;
    println!("   ✅ Number of products: {}", size);

    // RList example
    println!("\n3. RList (List):");
    let task_list = client.get_list::<String>("tasks");

    task_list.add(&"Task 1: Write documentation".to_string())?;
    task_list.add(&"Task 2: Fix bugs".to_string())?;
    task_list.add(&"Task 3: Write tests".to_string())?;
    println!("   ✅ Added 3 tasks");

    let tasks = task_list.range(0, -1)?;
    println!("   ✅ All tasks: {:?}", tasks);

    let first_task = task_list.pop_front()?;
    println!("   ✅ Popped first task: {:?}", first_task);

    // RSet example
    println!("\n4. RSet (Set):");
    let unique_tags = client.get_set::<String>("product:tags");

    unique_tags.add(&"electronics".to_string())?;
    unique_tags.add(&"computer".to_string())?;
    unique_tags.add(&"electronics".to_string())?; // Duplicate
    println!("   ✅ Added tags (including duplicates)");

    let tags = unique_tags.members()?;
    println!("   ✅ Unique tags: {:?}", tags);
    println!("   ✅ Number of tags: {}", tags.len());

    // RSortedSet example
    println!("\n5. RSortedSet (Sorted Set):");
    let leaderboard = client.get_sorted_set::<String>("game:leaderboard");

    leaderboard.add(&"player1".to_string(), 1500.0)?;
    leaderboard.add(&"player2".to_string(), 1800.0)?;
    leaderboard.add(&"player3".to_string(), 1200.0)?;
    println!("   ✅ Added player scores");

    let top_players = leaderboard.rev_range(0, 2)?;
    println!("   ✅ Top 3 players: {:?}", top_players);

    let player2_score = leaderboard.score(&"player2".to_string())?;
    println!("   ✅ player2 score: {:?}", player2_score);

    Ok(())
}

fn distributed_locks(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n🔒 Distributed Locks Example:");

    // 1. Basic lock
    println!("1. Basic Reentrant Lock:");
    let lock = client.get_lock("resource:update");

    println!("   Attempting to acquire lock...");
    lock.lock()?;
    println!("   ✅ Lock acquired successfully");

    // Simulate business operation
    thread::sleep(Duration::from_millis(100));
    println!("   🔧 Executing critical business operation...");

    lock.unlock()?;
    println!("   ✅ Lock released successfully");

    // 2. Try lock
    println!("\n2. Try Lock (with timeout):");
    let try_lock = client.get_lock("resource:try");

    let acquired = try_lock.try_lock_with_timeout(Duration::from_secs(1))?;
    if acquired {
        println!("   ✅ Successfully acquired lock");
        try_lock.unlock()?;
    } else {
        println!("   ⏱️  Lock acquisition timeout");
    }

    // 3. Fair lock
    println!("\n3. Fair Lock:");
    let fair_lock = client.get_fair_lock("resource:fair");

    fair_lock.lock()?;
    println!("   ✅ Fair lock acquired successfully");

    // Fair lock ensures acquisition in request order
    fair_lock.unlock()?;
    println!("   ✅ Fair lock released successfully");

    // 4. Read-write lock
    println!("\n4. Read-Write Lock:");
    let rw_lock = client.get_read_write_lock("resource:data", Duration::from_secs(60));

    // Acquire read lock
    let read_lock = rw_lock.read_lock();
    read_lock.lock()?;
    println!("   📖 Read lock acquired successfully (allows multiple readers)");
    read_lock.unlock()?;

    // Acquire write lock
    let write_lock = rw_lock.write_lock();
    write_lock.lock()?;
    println!("   ✍️  Write lock acquired successfully (exclusive)");
    write_lock.unlock()?;

    // 5. Red lock
    println!("\n5. Red Lock (RedLock):");
    let redlock_names = "lock:node1";

    let redlock = client.get_red_lock(redlock_names.to_string());
    redlock.lock()?;
    println!("   🔴 Red lock acquired successfully (majority consensus)");
    redlock.unlock()?;

    // 6. Built-in data structure locking
    println!("\n6. Built-in Data Structure Locking:");
    let data_bucket = client.get_bucket::<String>("shared:data");

    // Directly lock the entire data structure
    data_bucket.lock()?;
    data_bucket.set(&"locked data".to_string())?;
    data_bucket.unlock()?;
    println!("   ✅ Data structure locking used successfully");

    Ok(())
}

fn advanced_synchronizers(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n⚙️  Advanced Synchronizers Example:");

    // 1. Semaphore
    println!("1. Semaphore:");
    let semaphore = client.get_semaphore("api:rate:limit", 5);

    let acquired = semaphore.try_acquire(1, Duration::from_millis(100))?;
    if acquired {
        println!("   ✅ Acquired semaphore permit successfully");

        // Simulate API call
        thread::sleep(Duration::from_millis(50));
        println!("   📞 Executing API call...");

        semaphore.release(1)?;
        println!("   ✅ Released semaphore permit");
    }

    let available = semaphore.available_permits()?;
    println!("   📊 Available permits: {}", available);

    // 2. Rate limiter
    println!("\n2. Rate Limiter:");
    let rate_limiter = client.get_rate_limiter("api:limiter", 10.0, 20.0); // 10 req/s, capacity 20

    for i in 1..=15 {
        if rate_limiter.try_acquire(1.0)? {
            println!("   ✅ Request {}: Allowed", i);
        } else {
            println!("   🚫 Request {}: Rate limited", i);
        }
        thread::sleep(Duration::from_millis(50));
    }

    // 3. Counter
    println!("\n3. CountDownLatch:");
    let latch = client.get_count_down_latch("task:completion", 3);

    // Start multiple worker threads
    let latch_clone = latch.clone();
    let handle1 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        println!("   👷 Worker thread 1 completed task");
        latch_clone.count_down().unwrap();
    });

    let latch_clone = latch.clone();
    let handle2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        println!("   👷 Worker thread 2 completed task");
        latch_clone.count_down().unwrap();
    });

    let latch_clone = latch.clone();
    let handle3 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        println!("   👷 Worker thread 3 completed task");
        latch_clone.count_down().unwrap();
    });

    println!("   ⏳ Main thread waiting for all workers to complete...");
    latch.r#await(Some(Duration::from_secs(5)))?;
    println!("   ✅ All workers completed!");

    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();

    // 4. Atomic operations
    println!("\n4. Atomic Long:");
    let atomic_counter = client.get_atomic_long("global:counter");

    let initial = atomic_counter.get()?;
    println!("   📊 Initial value: {}", initial);

    let new_value = atomic_counter.increment_and_get()?;
    println!("   ➕ After increment: {}", new_value);

    let added = atomic_counter.add_and_get(10)?;
    println!("   🔟 After adding 10: {}", added);

    Ok(())
}

fn batch_operations(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n📚 Batch Operations Example:");

    // 1. Create batch operation
    let mut batch = &mut client.create_batch();

    // Add multiple operations
    for i in 1..=10 {
        let key = format!("batch:key:{}", i);
        let value = format!("value:{}", i);
        batch = batch.set(&key, &value);

        if i % 3 == 0 {
            batch = batch.get::<String>(key);
        }
    }

    println!("   📋 Added 10 SET operations and 3 GET operations");

    // 2. Execute batch operation
    let start = std::time::Instant::now();
    let results = batch.execute()?.unwrap_or_default();
    let duration = start.elapsed();

    println!("   ⚡ Batch execution completed, duration: {:?}", duration);
    println!("   📊 Number of results returned: {}", results.len());

    // 3. Analyze results
    let mut set_success = 0;
    let get_results = 0;

    for result in results {
        match result {
            BatchResult::Error(e) => {
                println!("   ❌ Batch operation error: {}", e);
            }
            _ => set_success += 1,
        }
    }

    println!("   ✅ SET operations successful: {}", set_success);
    println!("   ✅ GET results received: {}", get_results);

    Ok(())
}

fn transaction_operations(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n💳 Transaction Operations Example:");

    // Simulate bank transfer scenario
    println!("   🏦 Bank Transfer Scenario:");

    // Initialize account balances
    let alice_account = client.get_bucket::<i64>("account:alice");
    let bob_account = client.get_bucket::<i64>("account:bob");

    alice_account.set(&1000)?;
    bob_account.set(&500)?;

    println!("   📊 Before transfer - Alice: 1000, Bob: 500");

    // Use optimized transaction API
    let result = client.execute_transaction(|tx| {
        // Use closure to build transaction with auto-retry support
        let alice_balance: i64 = tx.query("account:alice")?;
        if alice_balance < 200 {
            return Err(RedissonError::InvalidOperation("Insufficient balance for Alice".to_string()));
        }

        let bob_balance: i64 = tx.query("account:bob").unwrap_or(0);

        tx.set("account:alice", &(alice_balance - 200))?
            .set("account:bob", &(bob_balance + 200))?
            .set("transaction:log", &"Transfer 200 from Alice to Bob".to_string())?;

        Ok(())
    });

    match result {
        Ok(()) => {
            println!("   ✅ Transfer successful!");

            let alice_after: i64 = alice_account.get()?.unwrap_or(0);
            let bob_after: i64 = bob_account.get()?.unwrap_or(0);

            println!("   📊 After transfer - Alice: {}, Bob: {}", alice_after, bob_after);
        }
        Err(e) => {
            println!("   ❌ Transfer failed: {}", e);

            // Show final balances (should be original values)
            let alice_final: i64 = alice_account.get()?.unwrap_or(0);
            let bob_final: i64 = bob_account.get()?.unwrap_or(0);
            println!("   📊 Final balances - Alice: {}, Bob: {}", alice_final, bob_final);
        }
    }

    Ok(())
}

fn pubsub_example(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n📢 Publish/Subscribe Example:");

    let topic = client.get_topic("chat:room:general");

    // Start subscriber thread
    let topic_clone = topic.clone();

    let subscriber_handle = thread::spawn(move || {
        println!("   👂 Subscriber started, waiting for messages...");

        topic_clone.add_listener_fn(|channel, message| {
            println!("   📩 Received message: {}", message);
        }).unwrap();

        // Keep subscription alive
        thread::sleep(Duration::from_secs(3));
    });

    // Wait for subscriber to be ready
    thread::sleep(Duration::from_millis(100));

    // Publish messages
    println!("   📤 Publishing messages...");
    topic.publish(&"Hello everyone!".to_string())?;
    thread::sleep(Duration::from_millis(100));

    topic.publish(&"How are you doing?".to_string())?;
    thread::sleep(Duration::from_millis(100));

    topic.publish(&"Goodbye!".to_string())?;

    // Wait for message processing
    thread::sleep(Duration::from_millis(500));

    subscriber_handle.join().unwrap();
    println!("   ✅ Publish/Subscribe example completed");

    Ok(())
}

fn delayed_queue_example(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n⏰ Delayed Queue Example:");

    let delayed_queue = client.get_delayed_queue::<String>("tasks:delayed");
    let task_queue = client.get_list::<String>("tasks:ready");

    // Add delayed tasks
    println!("   🕐 Adding delayed tasks (execution in 3 seconds)...");
    delayed_queue.offer(&"Process user data".to_string(), Duration::from_secs(3))?;

    delayed_queue.offer(&"Send email notification".to_string(), Duration::from_secs(5))?;

    delayed_queue.offer(&"Generate report".to_string(), Duration::from_secs(8))?;

    println!("   👀 Monitoring task queue...");

    // Monitor task queue
    let start_time = std::time::Instant::now();
    let mut completed_tasks = 0;

    while completed_tasks < 3 && start_time.elapsed() < Duration::from_secs(10) {
        if let Some(task) = task_queue.pop_front()? {
            println!("   ✅ Task executed: {} (delay: {:?})", task, start_time.elapsed());
            completed_tasks += 1;
        }
        thread::sleep(Duration::from_millis(100));
    }

    println!("   📊 Completed {} delayed tasks", completed_tasks);

    Ok(())
}

fn async_example() -> RedissonResult<()> {
    println!("\n⚡ Asynchronous Operations Example:");

    // Use Tokio runtime for async execution
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let config = RedissonConfig::single_server("redis://127.0.0.1:6379");
        let client = AsyncRedissonClient::new(config).await.unwrap();

        println!("   ✅ Async client created successfully");

        // Async lock
        let lock = client.get_lock("async:test");
        lock.lock().await.unwrap();
        println!("   🔒 Async lock acquired successfully");

        // Async data operations
        let bucket = client.get_bucket::<String>("async:data");
        bucket.set(&"Async value".to_string()).await.unwrap();

        let value = bucket.get().await.unwrap();
        println!("   📦 Async data: {:?}", value);

        // Async atomic operation
        let atomic = client.get_atomic_long("async:counter");
        let count = atomic.increment_and_get().await.unwrap();
        println!("   🔢 Async counter: {}", count);

        lock.unlock().await.unwrap();
        println!("   🔓 Async lock released successfully");

        client.shutdown().await.unwrap();
    });

    println!("   ✅ Asynchronous operations example completed");

    Ok(())
}