# Redisson

<p align="center">
  <img src="https://img.icons8.com/color/96/000000/redis.png" alt="Redis Logo" width="96" height="96">
</p>

<p align="center">
  <strong>A Redis-based distributed synchronization and data structures library for Rust</strong>
</p>

<p align="center">
  <a href="https://crates.io/crates/redisson">
    <img src="https://img.shields.io/crates/v/redisson.svg" alt="Crates.io">
  </a>
  <a href="https://docs.rs/redisson">
    <img src="https://docs.rs/redisson/badge.svg" alt="Documentation">
  </a>
  <a href="https://github.com/wslongchen/redisson/actions">
    <img src="https://github.com/wslongchen/redisson/actions/workflows/ci.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://github.com/wslongchen/redisson/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License MIT">
  </a>
  <a href="https://github.com/wslongchen/redisson">
    <img src="https://img.shields.io/badge/rustc-1.60%2B-lightgrey" alt="Minimum Rust">
  </a>
  <a href="https://github.com/wslongchen/redisson/stargazers">
    <img src="https://img.shields.io/github/stars/wslongchen/redisson" alt="GitHub stars">
  </a>
</p>

## 🎯 Features

- **🔒 Distributed Locks**: Reentrant locks, fair locks, read-write locks, and RedLock algorithm with automatic renewal
- **📊 Rich Data Structures**: Distributed maps, lists, sets, sorted sets, buckets, and streams
- **⚡ Dual Runtime**: Full support for both synchronous and asynchronous operations
- **🔄 Synchronization Primitives**: Semaphores, rate limiters, countdown latches, and atomic counters
- **📈 High Performance**: Connection pooling, command pipelining, and batch operations
- **💾 Local Cache Integration**: Read-through/write-through caching with local cache
- **🔧 Comprehensive Configuration**: Flexible configuration for various Redis deployment modes
- **🎯 Type Safety**: Full Rust type system support with compile-time checking
- **🛡️ Production Ready**: Automatic reconnection, timeout handling, and comprehensive error management
- **📡 Advanced Features**: Redis Stream support, delayed queues, and publish/subscribe messaging

## 📦 Installation

Add this to your `Cargo.toml`:

### Basic Installation
```toml
[dependencies]
redisson = "0.1"
```

### With Async Support (requires Tokio)
```toml
[dependencies]
redisson = { version = "0.1", features = ["async"] }
tokio = { version = "1", features = ["full"] }
```

### With Additional Features
```toml
[dependencies]
redisson = { version = "0.1", features = ["async", "caching", "streams"] }
```

## 🚀 Quick Start

### 1. Basic Synchronous Usage

```rust
use redisson::{RedissonClient, RedissonConfig};
use std::time::Duration;

fn main() -> redisson::RedissonResult<()> {
    // Create configuration
    let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
        .with_pool_size(10)
        .with_connection_timeout(Duration::from_secs(5));

    // Create client
    let client = RedissonClient::new(config)?;

    // Use distributed lock
    let lock = client.get_lock("my-resource");
    lock.lock()?;
    println!("Critical section accessed");
    lock.unlock()?;

    // Use distributed data structures
    let bucket = client.get_bucket::<String>("my-bucket");
    bucket.set(&"Hello World".to_string())?;
    let value: Option<String> = bucket.get()?;
    println!("Bucket value: {:?}", value);

    // Use distributed map
    let map = client.get_map::<String, i32>("my-map");
    map.put(&"key1".to_string(), &42)?;

    client.shutdown()?;
    Ok(())
}
```

### 2. Asynchronous Usage

```rust
use redisson::{AsyncRedissonClient, RedissonConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> redisson::RedissonResult<()> {
    // Create async configuration
    let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
        .with_pool_size(10);

    // Create async client
    let client = AsyncRedissonClient::new(config).await?;

    // Use async distributed lock
    let lock = client.get_lock("async-resource");
    lock.lock().await?;
    println!("Async critical section accessed");
    lock.unlock().await?;

    // Use async data operations
    let bucket = client.get_bucket::<String>("async-data");
    bucket.set(&"Async Value".to_string()).await?;
    let value = bucket.get().await?;
    println!("Async value: {:?}", value);

    client.shutdown().await?;
    Ok(())
}
```

### 3. Distributed Lock with Watchdog

```rust
use std::time::Duration;

fn lock_example(client: &RedissonClient) -> redisson::RedissonResult<()> {
    // Get a reentrant lock with automatic renewal
    let lock = client.get_lock("database-update");
    
    // Try to acquire lock with timeout
    if lock.try_lock_with_timeout(Duration::from_secs(5))? {
        // Lock acquired - watchdog will automatically renew the lock
        println!("Lock acquired, performing critical operations...");
        
        // Simulate long-running operation
        std::thread::sleep(Duration::from_secs(30));
        
        lock.unlock()?;
        println!("Lock released");
    } else {
        println!("Failed to acquire lock within timeout");
    }
    
    Ok(())
}
```

### 4. Redis Stream Support

```rust
use std::collections::HashMap;

fn stream_example(client: &RedissonClient) -> redisson::RedissonResult<()> {
    let stream = client.get_stream::<String>("orders:stream");
    
    // Create consumer group
    stream.create_group("order-processors", "0")?;
    
    // Add messages to stream
    let mut fields = HashMap::new();
    fields.insert("order".to_string(), "Order #1234".to_string());
    let message_id = stream.add_auto_id(&fields)?;
    println!("Message added with ID: {}", message_id);
    
    // Read messages from consumer group
    let messages = stream.read_group("order-processors", "consumer-1", Some(10), None, false)?;
    for message in messages {
        println!("Received message: {:?}", message);
    }
    
    Ok(())
}
```

### 5. Local Cache Integration

```rust
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct UserSession {
    user_id: String,
    session_token: String,
    last_activity: u64,
}

fn cache_example(client: &RedissonClient) -> redisson::RedissonResult<()> {
    // Create cache with local caching
    let user_cache = client.get_cache::<String, UserSession>("user_sessions");
    
    // Set data (will be cached locally)
    let session = UserSession {
        user_id: "user123".to_string(),
        session_token: "abc123def456".to_string(),
        last_activity: 1234567890,
    };
    
    user_cache.set("user123".to_string(), session.clone())?;
    
    // First read (may go to Redis)
    let cached = user_cache.get(&"user123".to_string())?;
    println!("First read: {:?}", cached);
    
    // Second read (from local cache - faster)
    let cached_again = user_cache.get(&"user123".to_string())?;
    println!("Second read: {:?}", cached_again);
    
    // Get cache statistics
    let cache_stats = user_cache.get_local_cache().get_stats();
    println!("Cache hits: {}", cache_stats.total_hits);
    
    Ok(())
}
```

## 📊 Complete Example: Order Processing System

```rust
use redisson::{RedissonClient, RedissonConfig};
use std::time::Duration;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Order {
    id: String,
    customer_id: String,
    amount: f64,
    status: OrderStatus,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
enum OrderStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

fn main() -> redisson::RedissonResult<()> {
    let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
        .with_pool_size(20)
        .with_connection_timeout(Duration::from_secs(5));

    let client = RedissonClient::new(config)?;

    // 1. Use distributed lock for order processing
    process_order_with_lock(&client)?;
    
    // 2. Use Redis Stream for order events
    publish_order_events(&client)?;
    
    // 3. Use cache for order data
    cache_order_data(&client)?;
    
    // 4. Use rate limiter for API calls
    rate_limit_api_calls(&client)?;
    
    client.shutdown()?;
    Ok(())
}

fn process_order_with_lock(client: &RedissonClient) -> RedissonResult<()> {
    let lock = client.get_lock("order:process:123");
    
    if lock.try_lock_with_timeout(Duration::from_secs(10))? {
        println!("Processing order 123...");
        // Simulate order processing
        std::thread::sleep(Duration::from_secs(2));
        lock.unlock()?;
        println!("Order processing completed");
    }
    
    Ok(())
}

fn publish_order_events(client: &RedissonClient) -> RedissonResult<()> {
    let order_stream = client.get_stream::<Order>("orders:events");
    
    let order = Order {
        id: "ORD-001".to_string(),
        customer_id: "CUST-001".to_string(),
        amount: 299.99,
        status: OrderStatus::Processing,
    };
    
    let mut fields = HashMap::new();
    fields.insert("order".to_string(), order);
    
    let message_id = order_stream.add_auto_id(&fields)?;
    println!("Order event published with ID: {}", message_id);
    
    Ok(())
}

fn cache_order_data(client: &RedissonClient) -> RedissonResult<()> {
    let order_cache = client.get_cache::<String, Order>("orders:cache");
    
    let order = Order {
        id: "ORD-001".to_string(),
        customer_id: "CUST-001".to_string(),
        amount: 299.99,
        status: OrderStatus::Pending,
    };
    
    order_cache.set("ORD-001".to_string(), order)?;
    
    // Subsequent reads will be faster with local cache
    let cached_order = order_cache.get(&"ORD-001".to_string())?;
    println!("Cached order: {:?}", cached_order);
    
    Ok(())
}

fn rate_limit_api_calls(client: &RedissonClient) -> RedissonResult<()> {
    let rate_limiter = client.get_rate_limiter("api:orders", 10.0, 20.0); // 10 req/s, burst 20
    
    for i in 1..=15 {
        if rate_limiter.try_acquire(1.0)? {
            println!("API call {}: Allowed", i);
        } else {
            println!("API call {}: Rate limited", i);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    
    Ok(())
}
```

## 🔧 Configuration

### Basic Configuration
```rust
use redisson::RedissonConfig;
use std::time::Duration;

let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
    .with_pool_size(20)                    // Connection pool size
    .with_connection_timeout(Duration::from_secs(5))
    .with_response_timeout(Duration::from_secs(3))
    .with_lock_expire_time(Duration::from_secs(30))
    .with_watchdog_timeout(Duration::from_secs(10))
    .with_retry_count(3)
    .with_drift_factor(0.01)               // Clock drift factor for RedLock
    .with_backup_pool_count(2);
```

### Redis Cluster Mode
```rust
let config = RedissonConfig::cluster(vec![
    "redis://127.0.0.1:7000",
    "redis://127.0.0.1:7001",
    "redis://127.0.0.1:7002",
])
.with_cluster_scan_interval(Duration::from_secs(5))
.with_pool_size(10);
```

### Redis Sentinel Mode
```rust
let config = RedissonConfig::sentinel(vec![
    "redis://127.0.0.1:26379",
    "redis://127.0.0.1:26380",
    "redis://127.0.0.1:26381",
], "mymaster")
.with_sentinel_password("password")
.with_database(0);
```

## 📊 Supported Data Structures

| Data Structure | Sync Support | Async Support | Description |
|----------------|--------------|---------------|-------------|
| **RBucket** | ✅ | ✅ | Simple key-value storage |
| **RMap** | ✅ | ✅ | Distributed hash map |
| **RList** | ✅ | ✅ | Distributed list |
| **RSet** | ✅ | ✅ | Distributed set |
| **RSortedSet** | ✅ | ✅ | Distributed sorted set |
| **RStream** | ✅ | ✅ | Redis Stream with consumer groups |
| **RLock** | ✅ | ✅ | Reentrant distributed lock |
| **RFairLock** | ✅ | ✅ | Fair distributed lock |
| **RReadWriteLock** | ✅ | ✅ | Read-write distributed lock |
| **RRedLock** | ✅ | ✅ | RedLock algorithm implementation |
| **RSemaphore** | ✅ | ✅ | Distributed semaphore |
| **RRateLimiter** | ✅ | ✅ | Distributed rate limiter |
| **RCountDownLatch** | ✅ | ✅ | Distributed countdown latch |
| **RAtomicLong** | ✅ | ✅ | Distributed atomic long |
| **RTopic** | ✅ | ✅ | Publish/subscribe messaging |
| **RDelayedQueue** | ✅ | ✅ | Delayed task queue |
| **RCache** | ✅ | ✅ | Local cache with Redis backend |
| **RBatch** | ✅ | ✅ | Batch operation support |

## 🛠️ Advanced Features

### Batch Operations
```rust
fn batch_operations(client: &RedissonClient) -> RedissonResult<()> {
    let mut batch = client.create_batch();
    
    // Add multiple operations
    for i in 1..=100 {
        batch = batch.set(&format!("key:{}", i), &format!("value:{}", i));
    }
    
    // Execute all operations in a single network call
    let results = batch.execute()?;
    println!("Batch executed with {} results", results.len());
    
    Ok(())
}
```

### Transaction Support
```rust
fn transaction_example(client: &RedissonClient) -> RedissonResult<()> {
    // Execute operations in a transaction
    let result = client.execute_transaction(|tx| {
        let balance: i64 = tx.query("account:balance")?;
        
        if balance >= 100 {
            tx.set("account:balance", &(balance - 100))?
                .set("transaction:log", &"Withdrawn 100".to_string())?;
            Ok(())
        } else {
            Err(redisson::RedissonError::InvalidOperation(
                "Insufficient balance".to_string()
            ))
        }
    });
    
    match result {
        Ok(_) => println!("Transaction successful"),
        Err(e) => println!("Transaction failed: {}", e),
    }
    
    Ok(())
}
```

### Watchdog Mechanism
The watchdog automatically renews locks to prevent premature expiration during long-running operations:

```rust
fn watchdog_example(client: &RedissonClient) -> RedissonResult<()> {
    let lock = client.get_lock("long-running-task");
    
    // Lock with automatic renewal
    lock.lock()?;
    
    // Long-running operation - watchdog will renew the lock
    for i in 1..=60 {
        println!("Processing step {}...", i);
        std::thread::sleep(Duration::from_secs(1));
        
        // Lock will be automatically renewed every 10 seconds
        // (configurable via RedissonConfig)
    }
    
    lock.unlock()?;
    Ok(())
}
```

## 📈 Performance Optimization

### 1. Connection Pooling
```rust
let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
    .with_pool_size(50)                    // Adjust based on workload
    .with_idle_timeout(Duration::from_secs(60));
```

### 2. Batch Operations for Bulk Data
```rust
// Instead of individual calls
for item in items {
    map.put(&item.key, &item.value)?;
}

// Use batch operations
let mut batch = client.create_batch();
for item in items {
    batch = batch.set(&item.key, &item.value);
}
batch.execute()?;
```

### 3. Local Cache for Read-Heavy Workloads
```rust
let cache = client.get_cache::<String, Data>("read-heavy-data")
    .with_local_cache_size(1000)           // Cache 1000 items locally
    .with_local_cache_ttl(Duration::from_secs(30));
```

### 4. Pipeline Configuration
```rust
let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
    .with_batch_config(BatchConfig::default()
        .with_max_batch_size(50)           // Optimal batch size
        .with_pipeline(true)               // Enable pipelining
        .with_max_wait_time(Duration::from_millis(10)));
```

## 🔍 Monitoring and Statistics

```rust
fn monitor_performance(client: &RedissonClient) -> RedissonResult<()> {
    let stats = client.get_stats();
    
    println!("Connection Pool Statistics:");
    println!("  Total connections: {}", stats.connection_stats.total_connections_created);
    println!("  Connection reuse rate: {:.1}%", stats.connection_stats.connection_reuse_rate());
    println!("  Peak connections: {}", stats.connection_stats.peak_connections);
    
    println!("\nCache Statistics:");
    println!("  Cache hit rate: {:.1}%", stats.cache_stats.avg_hit_rate * 100.0);
    println!("  Total hits: {}", stats.cache_stats.total_hits);
    println!("  Total misses: {}", stats.cache_stats.total_misses);
    
    println!("\nBatch Statistics:");
    println!("  Total batches: {}", stats.batch_stats.total_batches);
    println!("  Average batch size: {:.1}", stats.batch_stats.avg_batch_size);
    
    Ok(())
}
```

## 🧪 Testing

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use redisson::{RedissonClient, RedissonConfig};
    
    #[test]
    fn test_distributed_lock() -> RedissonResult<()> {
        let config = RedissonConfig::single_server("redis://127.0.0.1:6379");
        let client = RedissonClient::new(config)?;
        
        let lock = client.get_lock("test:lock");
        assert!(lock.try_lock()?);
        assert!(lock.is_locked()?);
        lock.unlock()?;
        assert!(!lock.is_locked()?);
        
        client.shutdown()?;
        Ok(())
    }
    
    #[test]
    fn test_cache_operations() -> RedissonResult<()> {
        let config = RedissonConfig::single_server("redis://127.0.0.1:6379");
        let client = RedissonClient::new(config)?;
        
        let cache = client.get_cache::<String, String>("test:cache");
        cache.set("key".to_string(), "value".to_string())?;
        
        let value = cache.get(&"key".to_string())?;
        assert_eq!(value, Some("value".to_string()));
        
        cache.clear()?;
        let cleared = cache.get(&"key".to_string())?;
        assert_eq!(cleared, None);
        
        client.shutdown()?;
        Ok(())
    }
}
```

### Benchmark Tests
```bash
# Run benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench lock_benchmark
cargo bench --bench cache_benchmark
cargo bench --bench batch_benchmark
```

## 🚀 Deployment

### Docker Deployment
```dockerfile
FROM rust:1.60 as builder
WORKDIR /usr/src/redisson
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/redisson/target/release/redisson-example /usr/local/bin/
CMD ["redisson-example"]
```

### Kubernetes Configuration
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redisson-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: redisson
  template:
    metadata:
      labels:
        app: redisson
    spec:
      containers:
      - name: redisson
        image: your-registry/redisson-app:latest
        env:
        - name: REDIS_URL
          value: "redis://redis-master:6379"
        - name: REDIS_POOL_SIZE
          value: "20"
        resources:
          requests:
            memory: "64Mi"
            cpu: "100m"
          limits:
            memory: "128Mi"
            cpu: "200m"
```

## 🔒 Security Best Practices

### 1. Secure Configuration
```rust
let config = RedissonConfig::single_server("redis://:password@secure-redis.example.com:6379")
    .with_connection_timeout(Duration::from_secs(10))
    .with_retry_count(3);
```

### 2. Connection Pool Security
```rust
let config = RedissonConfig::single_server("redis://127.0.0.1:6379")
    .with_pool_size(10)                    // Limit connection pool size
    .with_max_lifetime(Duration::from_secs(3600))  // Rotate connections periodically
    .with_idle_timeout(Duration::from_secs(300));   // Close idle connections
```

### 3. Redis ACL Support
```rust
let config = RedissonConfig::single_server("redis://username:password@127.0.0.1:6379")
    .with_database(0);                      // Use specific database
```

## 🤝 Contributing

We welcome contributions! Here's how you can help:

1. **Report Bugs**: Create an issue with detailed information
2. **Suggest Features**: Start a discussion about new features
3. **Submit PRs**: Follow our contributing guidelines
4. **Improve Documentation**: Help us make the docs better
5. **Add Examples**: Create useful examples for common use cases

### Development Setup
```bash
# Clone the repository
git clone https://github.com/wslongchen/redisson.git
cd redisson

# Run tests
cargo test

# Run tests with all features
cargo test --all-features

# Run benchmarks
cargo bench

# Build documentation
cargo doc --open

# Run examples
cargo run --example basic
cargo run --example async_example --features async
```

### Code Style
- Follow Rust conventions and clippy suggestions
- Use meaningful commit messages
- Add tests for new features
- Update documentation when adding features
- Keep API consistent and backward-compatible


## 📄 License
Licensed under either of:

+ Apache License, Version 2.0 (LICENSE-APACHE)

+ MIT license (LICENSE-MIT)

at your option.

## 🙏 Acknowledgments

+ Thanks to all contributors who have helped shape Akita

+ Inspired by great ORMs like Diesel, SQLx, and MyBatis

+ Built with ❤️ by the Cat&Dog Lab team

## 📞 Contact

+ Author: Mr.Pan

+ Email: 1049058427@qq.com

+ GitHub: @wslongchen

+ Project: Akita on GitHub

<p align="center">
  Made with ❤️ by the <a href="https://github.com/wslongchen">Mr.Pan</a> and the Cat&Dog Lab Team </p>
</p>