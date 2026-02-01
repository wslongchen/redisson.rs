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
    // 1. 配置客户端
    let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
        .with_pool_size(20)
        .with_connection_timeout(Duration::from_secs(5))
        .with_response_timeout(Duration::from_secs(3))
        .with_lock_expire_time(Duration::from_secs(30))
        .with_watchdog_timeout(Duration::from_secs(10))
        .with_retry_count(3)
        .with_drift_factor(0.01);

    println!("🚀 正在创建Redisson客户端...");

    // 2. 创建同步客户端
    let client = RedissonClient::new(config)?;
    println!("✅ 客户端创建成功");

    // 3. 基本数据结构使用示例
    basic_data_structures(&client)?;

    // 4. 分布式锁使用示例
    distributed_locks(&client)?;

    // 5. 高级同步器使用示例
    advanced_synchronizers(&client)?;

    // 6. 批量操作示例
    batch_operations(&client)?;

    // 7. 事务操作示例
    transaction_operations(&client)?;

    // 8. 发布订阅示例
    pubsub_example(&client)?;

    // 9. 延迟队列示例
    delayed_queue_example(&client)?;

    // 10. 异步操作示例
    async_example()?;

    println!("\n🎉 所有示例执行完成!");

    // 清理资源
    client.shutdown()?;
    println!("🔌 客户端已关闭");

    Ok(())
}

fn basic_data_structures(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n📦 基本数据结构示例:");

    // RBucket 示例
    println!("1. RBucket (键值对):");
    let bucket = client.get_bucket::<User>("user:alice");

    let alice = User {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        roles: vec!["admin".to_string(), "user".to_string()],
    };

    bucket.set(&alice)?;
    println!("   ✅ 设置用户数据");

    let retrieved: Option<User> = bucket.get()?;
    println!("   ✅ 获取用户数据: {:?}", retrieved.map(|u| u.name));

    // 设置过期时间
    bucket.set_with_ttl(&alice, Duration::from_secs(60))?;
    println!("   ✅ 设置60秒过期时间");

    // RMap 示例
    println!("\n2. RMap (哈希表):");
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
    println!("   ✅ 添加2个产品");

    let laptop_retrieved = product_map.get(&"p001".to_string())?;
    println!("   ✅ 获取产品p001: {:?}", laptop_retrieved.map(|p| p.name));

    let size = product_map.size()?;
    println!("   ✅ 产品数量: {}", size);

    // RList 示例
    println!("\n3. RList (列表):");
    let task_list = client.get_list::<String>("tasks");

    task_list.add(&"Task 1: Write documentation".to_string())?;
    task_list.add(&"Task 2: Fix bugs".to_string())?;
    task_list.add(&"Task 3: Write tests".to_string())?;
    println!("   ✅ 添加3个任务");

    let tasks = task_list.range(0, -1)?;
    println!("   ✅ 所有任务: {:?}", tasks);

    let first_task = task_list.pop_front()?;
    println!("   ✅ 弹出第一个任务: {:?}", first_task);

    // RSet 示例
    println!("\n4. RSet (集合):");
    let unique_tags = client.get_set::<String>("product:tags");

    unique_tags.add(&"electronics".to_string())?;
    unique_tags.add(&"computer".to_string())?;
    unique_tags.add(&"electronics".to_string())?; // 重复项
    println!("   ✅ 添加标签(包含重复项)");

    let tags = unique_tags.members()?;
    println!("   ✅ 唯一标签: {:?}", tags);
    println!("   ✅ 标签数量: {}", tags.len());

    // RSortedSet 示例
    println!("\n5. RSortedSet (有序集合):");
    let leaderboard = client.get_sorted_set::<String>("game:leaderboard");

    leaderboard.add(&"player1".to_string(), 1500.0)?;
    leaderboard.add(&"player2".to_string(), 1800.0)?;
    leaderboard.add(&"player3".to_string(), 1200.0)?;
    println!("   ✅ 添加玩家分数");

    let top_players = leaderboard.rev_range(0, 2)?;
    println!("   ✅ 排行榜前3名: {:?}", top_players);

    let player2_score = leaderboard.score(&"player2".to_string())?;
    println!("   ✅ player2分数: {:?}", player2_score);

    Ok(())
}

fn distributed_locks(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n🔒 分布式锁示例:");

    // 1. 基本锁
    println!("1. 基本可重入锁:");
    let lock = client.get_lock("resource:update");

    println!("   尝试获取锁...");
    lock.lock()?;
    println!("   ✅ 锁获取成功");

    // 模拟业务操作
    thread::sleep(Duration::from_millis(100));
    println!("   🔧 执行关键业务操作...");

    lock.unlock()?;
    println!("   ✅ 锁释放成功");

    // 2. 尝试锁
    println!("\n2. 尝试锁 (带超时):");
    let try_lock = client.get_lock("resource:try");

    let acquired = try_lock.try_lock_with_timeout(Duration::from_secs(1))?;
    if acquired {
        println!("   ✅ 成功获取锁");
        try_lock.unlock()?;
    } else {
        println!("   ⏱️  获取锁超时");
    }

    // 3. 公平锁
    println!("\n3. 公平锁:");
    let fair_lock = client.get_fair_lock("resource:fair");

    fair_lock.lock()?;
    println!("   ✅ 公平锁获取成功");

    // 公平锁保证按请求顺序获取锁
    fair_lock.unlock()?;
    println!("   ✅ 公平锁释放成功");

    // 4. 读写锁
    println!("\n4. 读写锁:");
    let rw_lock = client.get_read_write_lock("resource:data", Duration::from_secs(60));

    // 获取读锁
    let read_lock = rw_lock.read_lock();
    read_lock.lock()?;
    println!("   📖 读锁获取成功 (允许多个读)");
    read_lock.unlock()?;

    // 获取写锁
    let write_lock = rw_lock.write_lock();
    write_lock.lock()?;
    println!("   ✍️  写锁获取成功 (独占)");
    write_lock.unlock()?;

    // 5. 红锁
    println!("\n5. 红锁 (RedLock):");
    let redlock_names = "lock:node1";

    let redlock = client.get_red_lock(redlock_names.to_string());
    redlock.lock()?;
    println!("   🔴 红锁获取成功 (多数节点同意)");
    redlock.unlock()?;

    // 6. 数据结构自带锁
    println!("\n6. 数据结构自带锁:");
    let data_bucket = client.get_bucket::<String>("shared:data");

    // 直接锁住整个数据结构
    data_bucket.lock()?;
    data_bucket.set(&"locked data".to_string())?;
    data_bucket.unlock()?;
    println!("   ✅ 数据结构锁使用成功");

    Ok(())
}

fn advanced_synchronizers(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n⚙️  高级同步器示例:");

    // 1. 信号量
    println!("1. 信号量 (Semaphore):");
    let semaphore = client.get_semaphore("api:rate:limit", 5);

    let acquired = semaphore.try_acquire(1, Duration::from_millis(100))?;
    if acquired {
        println!("   ✅ 获取信号量许可成功");

        // 模拟API调用
        thread::sleep(Duration::from_millis(50));
        println!("   📞 执行API调用...");

        semaphore.release(1)?;
        println!("   ✅ 释放信号量许可");
    }

    let available = semaphore.available_permits()?;
    println!("   📊 可用许可数: {}", available);

    // 2. 限流器
    println!("\n2. 限流器 (Rate Limiter):");
    let rate_limiter = client.get_rate_limiter("api:limiter", 10.0, 20.0); // 10 req/s, 容量20

    for i in 1..=15 {
        if rate_limiter.try_acquire(1.0)? {
            println!("   ✅ 请求 {}: 允许通过", i);
        } else {
            println!("   🚫 请求 {}: 被限流", i);
        }
        thread::sleep(Duration::from_millis(50));
    }

    // 3. 计数器
    println!("\n3. 倒计数器 (CountDownLatch):");
    let latch = client.get_count_down_latch("task:completion", 3);

    // 启动多个工作线程
    let latch_clone = latch.clone();
    let handle1 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        println!("   👷 工作线程1完成任务");
        latch_clone.count_down().unwrap();
    });

    let latch_clone = latch.clone();
    let handle2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        println!("   👷 工作线程2完成任务");
        latch_clone.count_down().unwrap();
    });

    let latch_clone = latch.clone();
    let handle3 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        println!("   👷 工作线程3完成任务");
        latch_clone.count_down().unwrap();
    });

    println!("   ⏳ 主线程等待所有工作完成...");
    latch.r#await(Some(Duration::from_secs(5)))?;
    println!("   ✅ 所有工作完成!");

    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();

    // 4. 原子操作
    println!("\n4. 原子长整型:");
    let atomic_counter = client.get_atomic_long("global:counter");

    let initial = atomic_counter.get()?;
    println!("   📊 初始值: {}", initial);

    let new_value = atomic_counter.increment_and_get()?;
    println!("   ➕ 递增后: {}", new_value);

    let added = atomic_counter.add_and_get(10)?;
    println!("   🔟 加10后: {}", added);

    Ok(())
}

fn batch_operations(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n📚 批量操作示例:");

    // 1. 创建批量操作
    let mut batch = &mut client.create_batch();

    // 添加多个操作
    for i in 1..=10 {
        let key = format!("batch:key:{}", i);
        let value = format!("value:{}", i);
        batch = batch.set(&key, &value);

        if i % 3 == 0 {
            batch = batch.get::<String>(key);
        }
    }

    println!("   📋 添加了10个SET操作和3个GET操作");

    // 2. 执行批量操作
    let start = std::time::Instant::now();
    let results = batch.execute()?.unwrap_or_default();
    let duration = start.elapsed();

    println!("   ⚡ 批量执行完成，耗时: {:?}", duration);
    println!("   📊 返回结果数量: {}", results.len());

    // 3. 分析结果
    let mut set_success = 0;

    for result in results {
        match result {
            BatchResult::Error{..} => set_success += 1,
            _ => {}
        }
    }

    println!("   ✅ SET成功: {} 个", set_success);
    println!("   ✅ GET结果: {} 个", set_success);

    Ok(())
}

fn transaction_operations(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n💳 事务操作示例:");

    // 模拟银行转账场景
    println!("   🏦 银行转账场景:");

    // 初始化账户余额
    let alice_account = client.get_bucket::<i64>("account:alice");
    let bob_account = client.get_bucket::<i64>("account:bob");

    alice_account.set(&1000)?;
    bob_account.set(&500)?;

    println!("   📊 转账前 - Alice: 1000, Bob: 500");

    // 使用优化的事务API
    let result = client.execute_transaction(|tx| {
        // 这里使用闭包来构建事务，支持自动重试
        let alice_balance: i64 = tx.query("account:alice")?;
        if alice_balance < 200 {
            return Err(RedissonError::InvalidOperation("Alice余额不足".to_string()));
        }

        let bob_balance: i64 = tx.query("account:bob").unwrap_or(0);

        tx.set("account:alice", &(alice_balance - 200))?
            .set("account:bob", &(bob_balance + 200))?
            .set("transaction:log", &"Transfer 200 from Alice to Bob".to_string())?;

        Ok(())
    });

    match result {
        Ok(()) => {
            println!("   ✅ 转账成功!");

            let alice_after: i64 = alice_account.get()?.unwrap_or(0);
            let bob_after: i64 = bob_account.get()?.unwrap_or(0);

            println!("   📊 转账后 - Alice: {}, Bob: {}", alice_after, bob_after);
        }
        Err(e) => {
            println!("   ❌ 转账失败: {}", e);

            // 显示最终余额（应该是原始值）
            let alice_final: i64 = alice_account.get()?.unwrap_or(0);
            let bob_final: i64 = bob_account.get()?.unwrap_or(0);
            println!("   📊 最终余额 - Alice: {}, Bob: {}", alice_final, bob_final);
        }
    }

    Ok(())
}

fn pubsub_example(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n📢 发布订阅示例:");

    let topic = client.get_topic("chat:room:general");

    // 启动订阅者线程
    let topic_clone = topic.clone();
    
    let subscriber_handle = thread::spawn(move || {
        println!("   👂 订阅者启动，等待消息...");

        topic_clone.add_listener_fn(|channel, message| {
            println!("   📩 收到消息: {}", message);
        }).unwrap();

        // 保持订阅
        thread::sleep(Duration::from_secs(3));
    });

    // 等待订阅者就绪
    thread::sleep(Duration::from_millis(100));

    // 发布消息
    println!("   📤 发布消息...");
    topic.publish(&"Hello everyone!".to_string())?;
    thread::sleep(Duration::from_millis(100));

    topic.publish(&"How are you doing?".to_string())?;
    thread::sleep(Duration::from_millis(100));

    topic.publish(&"Goodbye!".to_string())?;

    // 等待消息处理
    thread::sleep(Duration::from_millis(500));

    subscriber_handle.join().unwrap();
    println!("   ✅ 发布订阅示例完成");

    Ok(())
}

fn delayed_queue_example(client: &RedissonClient) -> RedissonResult<()> {
    println!("\n⏰ 延迟队列示例:");

    let delayed_queue = client.get_delayed_queue::<String>("tasks:delayed");
    let task_queue = client.get_list::<String>("tasks:ready");

    // 添加延迟任务
    println!("   🕐 添加延迟任务 (3秒后执行)...");
    delayed_queue.offer(&"Process user data".to_string(), Duration::from_secs(3))?;

    delayed_queue.offer(&"Send email notification".to_string(), Duration::from_secs(5))?;

    delayed_queue.offer(&"Generate report".to_string(), Duration::from_secs(8))?;

    println!("   👀 监控任务队列...");

    // 监控任务队列
    let start_time = std::time::Instant::now();
    let mut completed_tasks = 0;

    while completed_tasks < 3 && start_time.elapsed() < Duration::from_secs(10) {
        if let Some(task) = task_queue.pop_front()? {
            println!("   ✅ 任务执行: {} (延迟: {:?})", task, start_time.elapsed());
            completed_tasks += 1;
        }
        thread::sleep(Duration::from_millis(100));
    }

    println!("   📊 完成 {} 个延迟任务", completed_tasks);

    Ok(())
}

fn async_example() -> RedissonResult<()> {
    println!("\n⚡ 异步操作示例:");

    // 使用Tokio运行时执行异步代码
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let config = RedissonConfig::single_server("redis://127.0.0.1:6379");
        let client = AsyncRedissonClient::new(config).await.unwrap();

        println!("   ✅ 异步客户端创建成功");

        // 异步锁
        let lock = client.get_lock("async:test");
        lock.lock().await.unwrap();
        println!("   🔒 异步锁获取成功");

        // 异步数据操作
        let bucket = client.get_bucket::<String>("async:data");
        bucket.set(&"Async value".to_string()).await.unwrap();

        let value = bucket.get().await.unwrap();
        println!("   📦 异步数据: {:?}", value);

        // 异步原子操作
        let atomic = client.get_atomic_long("async:counter");
        let count = atomic.increment_and_get().await.unwrap();
        println!("   🔢 异步计数器: {}", count);

        lock.unlock().await.unwrap();
        println!("   🔓 异步锁释放成功");

        client.shutdown().await.unwrap();
    });

    println!("   ✅ 异步操作示例完成");

    Ok(())
}
