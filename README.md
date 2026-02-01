#redisson

# 1. 安装依赖
cargo add redis r2d2 deadpool tokio serde serde_json uuid tracing thiserror

# 2. 运行示例
cargo run --example complete_example

# 3. 运行基准测试
cargo bench

# 4. 运行压力测试
cargo run --release --bin benchmark

# 5. 运行单元测试
cargo test -- --nocapture

# 6. 使用Criterion进行详细基准测试
cargo bench --bench criterion_bench
