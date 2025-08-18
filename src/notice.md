## database initial
未来迁移示例：migrations/20250819_add_column.sql如果您稍后向hot_tokens添加一列：SQL
ALTER TABLE hot_tokens ADD COLUMN price_usd BIGINT;
需要安装openssl
sudo apt-get install openssl
典型用途：大多数迁移都会添加/修改表（例如CREATE TABLE、ALTER TABLE ADD COLUMN）来保存数据。例如，更新hot_tokens并添加price_usd所有现有行都会保留。
您的情况：由于hot_tokensON CONFLICT进行更新，因此数据会频繁刷新，但元数据（例如，令牌/NFT 详细信息）可能需要保留。sqlx::migrate确保两个表都会发展而不会丢失数据，除非明确删除。
路径：migrations相对于运行时二进制文件的工作目录（例如，如果从/app/migrations）。
Docker：将migrations/到Dockerfile中的/app/migrations。
二进制：在分发中迁移/
sqlx::migrate：保存数据、应用增量更改并跟踪_sqlx_migrations。




Dockerfile包括迁移目录：dockerfile

FROM rust:1.80 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libpq-dev ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/your_binary_name /app/your_binary_name
COPY migrations /app/migrations
CMD ["/app/your_binary_name"]



