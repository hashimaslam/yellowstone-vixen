use clickhouse::{sql::Identifier, Client, Row};
use serde::{Deserialize, Serialize};
use std::time::UNIX_EPOCH;
use tracing;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{Duration, Instant};
use std::sync::Arc;
use std::collections::VecDeque;
use dotenv::dotenv;

use yellowstone_vixen as vixen;
use yellowstone_vixen_parser::token_holders::{AccountStats, TokenBalanceOutput};

#[derive(Debug, Row, Serialize, Deserialize, Clone)]
pub struct TokenBalanceEventRow {
    pub owner: String,
    pub mint: String,
    pub token_account: String,
    pub post_balance: f64,
    pub block_slot: u64,
    pub ts: i64,               // DateTime64(9) as i64 (nanos since epoch)
    pub signature: String,
    pub is_off_curve: bool,
}

#[derive(Debug, Row, Serialize, Deserialize, Clone)]
pub struct TokenBalanceLatestRow {
    pub owner: String,
    pub mint: String,
    pub token_account: String,
    pub post_balance: f64,
    pub block_slot: u64,
    pub ts: i64,
    pub signature: String,
    pub is_off_curve: bool,
}

#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    pub max_queue_size: usize,
    pub batch_size: usize,
    pub max_batch_delay: Duration,
    pub max_concurrent_inserts: usize,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 1_000_000,   // Much larger queue for burst handling
            batch_size: 10_000,          // Larger batches to reduce overhead
            max_batch_delay: Duration::from_millis(100), // Slightly longer to accumulate bigger batches
            max_concurrent_inserts: 16,  // More parallelism
        }
    }
}

#[derive(Clone)]
pub struct ClickHouseAsyncHandler {
    client: Client,
    raw_table: String,    // e.g. balance_portfolio_events
    latest_table: String, // e.g. balance_portfolio_latest
    mv_name: String,      // e.g. balance_portfolio_latest_mv
    sender: Arc<RwLock<Option<mpsc::UnboundedSender<Vec<AccountStats>>>>>,
    config: BackpressureConfig,
}

impl std::fmt::Debug for ClickHouseAsyncHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseAsyncHandler")
            .field("raw_table", &self.raw_table)
            .field("latest_table", &self.latest_table)
            .field("mv_name", &self.mv_name)
            .field("config", &self.config)
            .finish()
    }
}

impl ClickHouseAsyncHandler {
    pub fn new(url: &str, database: &str, base_table: &str) -> Self {
        Self::with_config(url, database, base_table, BackpressureConfig::default())
    }

    pub fn with_config(url: &str, database: &str, base_table: &str, config: BackpressureConfig) -> Self {
        let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
        let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "".into());

        // High-performance ClickHouse settings
        let client = Client::default()
            .with_url(url)
            .with_user(&user)
            .with_password(&password)
            .with_database(database)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0")  // Critical: don't wait
            .with_option("async_insert_busy_timeout_ms", "200") // Longer batching
            .with_option("async_insert_max_data_size", "10485760") // 10MB batches
            .with_option("async_insert_max_query_number", "1000") // More queries per batch
            .with_option("max_insert_threads", "32")  // More insert threads
            .with_option("max_insert_block_size", "10000000"); // Larger blocks

        Self {
            client,
            raw_table: format!("{}_events", base_table),
            latest_table: format!("{}_latest", base_table),
            mv_name: format!("{}_latest_mv", base_table),
            sender: Arc::new(RwLock::new(None)),
            config,
        }
    }

    /// Start the high-performance background ingestion pipeline
    pub async fn start_background_ingestion(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<AccountStats>>();

        {
            let mut sender_lock = self.sender.write().await;
            *sender_lock = Some(tx);
        }

        let client = self.client.clone();
        let raw_table = self.raw_table.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut buffer = VecDeque::new();
            let mut last_flush = Instant::now();
            let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_inserts));

            loop {
                tokio::select! {
                    Some(batch) = rx.recv() => {
                        // Add to buffer
                        buffer.extend(batch);

                        // Check flush conditions
                        let should_flush = buffer.len() >= config.batch_size
                            || last_flush.elapsed() >= config.max_batch_delay;

                        if should_flush && !buffer.is_empty() {
                            let batch_to_process: Vec<_> = buffer.drain(..config.batch_size.min(buffer.len())).collect();
                            let permit = semaphore.clone().acquire_owned().await;
                            let client_clone = client.clone();
                            let table_clone = raw_table.clone();

                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(e) = Self::insert_batch(&client_clone, &table_clone, &batch_to_process).await {
                                    tracing::error!("Background insert failed: {}", e);
                                } else {
                                    tracing::debug!("Inserted batch of {} rows", batch_to_process.len());
                                }
                            });

                            last_flush = Instant::now();
                        }
                    }

                    _ = tokio::time::sleep(config.max_batch_delay) => {
                        if !buffer.is_empty() && last_flush.elapsed() >= config.max_batch_delay {
                            let batch_to_process: Vec<_> = buffer.drain(..).collect();
                            let permit = semaphore.clone().acquire_owned().await;
                            let client_clone = client.clone();
                            let table_clone = raw_table.clone();

                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(e) = Self::insert_batch(&client_clone, &table_clone, &batch_to_process).await {
                                    tracing::error!("Background timeout insert failed: {}", e);
                                }
                            });

                            last_flush = Instant::now();
                        }
                    }
                }
            }
        });

        tracing::info!("🚀 High-performance background ingestion started for: {}", self.raw_table);
        Ok(())
    }

    async fn insert_batch(
        client: &Client,
        table: &str,
        balances: &[AccountStats],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if balances.is_empty() {
            return Ok(());
        }

        let ts = now();
        let mut insert = client.insert::<TokenBalanceEventRow>(table)?;

        for stats in balances {
            let row = TokenBalanceEventRow {
                owner: stats.owner.clone(),
                mint: stats.mint.clone(),
                token_account: stats.token_account.clone(),
                post_balance: stats.post_balance,
                block_slot: stats.block_slot,
                ts,
                signature: stats.signature.clone(),
                is_off_curve: stats.is_off_curve,
            };
            insert.write(&row).await?;
        }

        insert.end().await?;
        Ok(())
    }

    /// Create optimized schema for billion+ row performance
    pub async fn create_schema(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("🏗️  Creating optimized schema for billion+ rows...");

        // 1) Raw events table - optimized for high-speed writes
        let create_raw = r#"
            CREATE TABLE IF NOT EXISTS ?
            (
                owner          LowCardinality(String),
                mint           LowCardinality(String),
                token_account  String,
                post_balance   Float64,
                block_slot     UInt64,
                ts             DateTime64(9),
                signature      String,
                is_off_curve   Bool
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMMDD(ts)
            ORDER BY (owner, mint, token_account, block_slot)
            SETTINGS index_granularity = 4096,
                     max_parts_in_total = 100000,
                     merge_max_block_size = 8192,
                     parts_to_delay_insert = 300,
                     parts_to_throw_insert = 3000,
                     max_bytes_to_merge_at_max_space_in_pool = 161061273600
        "#;

        self.client.query(create_raw)
            .bind(Identifier(&self.raw_table))
            .execute().await?;

        // 2) Latest aggregating table - optimized for fast queries
        let create_latest = r#"
            CREATE TABLE IF NOT EXISTS ?
            (
                owner          LowCardinality(String),
                mint           LowCardinality(String),
                token_account  String,
                post_balance_state AggregateFunction(argMax, Float64, UInt64),
                signature_state    AggregateFunction(argMax, String, UInt64),
                ts_state           AggregateFunction(argMax, DateTime64(9), UInt64),
                is_off_curve_state AggregateFunction(argMax, Bool, UInt64),
                slot_state         AggregateFunction(max, UInt64)
            )
            ENGINE = AggregatingMergeTree
            ORDER BY (owner, mint, token_account)
            PRIMARY KEY (owner, mint)
            SETTINGS index_granularity = 4096,
                     merge_max_block_size = 8192
        "#;

        self.client.query(create_latest)
            .bind(Identifier(&self.latest_table))
            .execute().await?;

        // 3) Materialized view for real-time aggregation
        let create_mv = r#"
            CREATE MATERIALIZED VIEW IF NOT EXISTS ? TO ?
            AS
            SELECT
                owner,
                mint,
                token_account,
                argMaxState(post_balance, block_slot) AS post_balance_state,
                argMaxState(signature, block_slot) AS signature_state,
                argMaxState(ts, block_slot) AS ts_state,
                argMaxState(is_off_curve, block_slot) AS is_off_curve_state,
                maxState(block_slot) AS slot_state
            FROM ?
            GROUP BY owner, mint, token_account
        "#;

        self.client.query(create_mv)
            .bind(Identifier(&self.mv_name))
            .bind(Identifier(&self.latest_table))
            .bind(Identifier(&self.raw_table))
            .execute().await?;

        // 4) Add performance indexes
        let create_indexes = vec![
            format!("ALTER TABLE {} ADD INDEX IF NOT EXISTS idx_mint_owner (mint, owner) TYPE bloom_filter(0.01) GRANULARITY 4096", self.latest_table),
            format!("ALTER TABLE {} ADD INDEX IF NOT EXISTS idx_owner (owner) TYPE bloom_filter(0.01) GRANULARITY 4096", self.latest_table),
        ];

        for index_sql in create_indexes {
            let _ = self.client.query(&index_sql).execute().await; // Ignore errors if already exist
        }

        tracing::info!("✅ Schema created: raw='{}', latest='{}', mv='{}'", self.raw_table, self.latest_table, self.mv_name);
        Ok(())
    }

    /// Lightning-fast query: Get latest balance for specific owner + mint
    pub async fn get_owner_mint_balance(
        &self,
        owner: &str,
        mint: &str,
    ) -> Result<Option<TokenBalanceLatestRow>, Box<dyn std::error::Error + Send + Sync>> {
        let q = r#"
            SELECT
                owner,
                mint,
                token_account,
                argMaxMerge(post_balance_state) AS post_balance,
                maxMerge(slot_state) AS block_slot,
                argMaxMerge(ts_state) AS ts,
                argMaxMerge(signature_state) AS signature,
                argMaxMerge(is_off_curve_state) AS is_off_curve
            FROM ?
            WHERE owner = ? AND mint = ?
            GROUP BY owner, mint, token_account
            LIMIT 1
        "#;

        let rows = self.client.query(q)
            .bind(Identifier(&self.latest_table))
            .bind(owner)
            .bind(mint)
            .fetch_all::<TokenBalanceLatestRow>().await?;

        Ok(rows.into_iter().next())
    }

    /// Fast query: Get all token balances for an owner
    pub async fn get_owner_all_balances(
        &self,
        owner: &str,
    ) -> Result<Vec<TokenBalanceLatestRow>, Box<dyn std::error::Error + Send + Sync>> {
        let q = r#"
            SELECT
                owner,
                mint,
                token_account,
                argMaxMerge(post_balance_state) AS post_balance,
                maxMerge(slot_state) AS block_slot,
                argMaxMerge(ts_state) AS ts,
                argMaxMerge(signature_state) AS signature,
                argMaxMerge(is_off_curve_state) AS is_off_curve
            FROM ?
            WHERE owner = ?
            GROUP BY owner, mint, token_account
            ORDER BY mint
        "#;

        let rows = self.client.query(q)
            .bind(Identifier(&self.latest_table))
            .bind(owner)
            .fetch_all::<TokenBalanceLatestRow>().await?;
        Ok(rows)
    }

    /// Batch query: Get specific mint balance for multiple owners
    pub async fn get_multiple_owners_mint_balance(
        &self,
        owners: &[String],
        mint: &str,
    ) -> Result<Vec<TokenBalanceLatestRow>, Box<dyn std::error::Error + Send + Sync>> {
        if owners.is_empty() {
            return Ok(vec![]);
        }

        // For large batches, chunk into smaller queries to avoid recursion
        if owners.len() > 1000 {
            let mut results = Vec::new();
            for chunk in owners.chunks(1000) {
                let chunk_results = self.execute_owners_mint_query(chunk, mint).await?;
                results.extend(chunk_results);
            }
            return Ok(results);
        }

        // For smaller batches, execute directly
        self.execute_owners_mint_query(owners, mint).await
    }

    /// Execute the actual query for a chunk of owners (non-recursive helper)
    async fn execute_owners_mint_query(
        &self,
        owners: &[String],
        mint: &str,
    ) -> Result<Vec<TokenBalanceLatestRow>, Box<dyn std::error::Error + Send + Sync>> {
        let owner_list = owners.iter()
            .map(|o| format!("'{}'", o.replace("'", "''")))
            .collect::<Vec<_>>()
            .join(",");

        let q = format!(r#"
            SELECT
                owner,
                mint,
                token_account,
                argMaxMerge(post_balance_state) AS post_balance,
                maxMerge(slot_state) AS block_slot,
                argMaxMerge(ts_state) AS ts,
                argMaxMerge(signature_state) AS signature,
                argMaxMerge(is_off_curve_state) AS is_off_curve
            FROM {}
            WHERE owner IN ({}) AND mint = ?
            GROUP BY owner, mint, token_account
            ORDER BY owner
        "#, self.latest_table, owner_list);

        let rows = self.client.query(&q)
            .bind(mint)
            .fetch_all::<TokenBalanceLatestRow>().await?;
        Ok(rows)
    }

    /// Query: Get top holders of a specific mint
    pub async fn get_mint_top_holders(
        &self,
        mint: &str,
        min_balance: f64,
        limit: u64,
    ) -> Result<Vec<TokenBalanceLatestRow>, Box<dyn std::error::Error + Send + Sync>> {
        let q = format!(r#"
            SELECT
                owner,
                mint,
                token_account,
                argMaxMerge(post_balance_state) AS post_balance,
                maxMerge(slot_state) AS block_slot,
                argMaxMerge(ts_state) AS ts,
                argMaxMerge(signature_state) AS signature,
                argMaxMerge(is_off_curve_state) AS is_off_curve
            FROM {}
            WHERE mint = ?
            GROUP BY owner, mint, token_account
            HAVING post_balance >= ?
            ORDER BY post_balance DESC
            LIMIT {}
        "#, self.latest_table, limit);

        let rows = self.client.query(&q)
            .bind(mint)
            .bind(min_balance)
            .fetch_all::<TokenBalanceLatestRow>().await?;
        Ok(rows)
    }

    /// Get pipeline health stats
    pub async fn get_table_stats(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let q = format!(r#"
            SELECT
                table,
                formatReadableSize(total_bytes) as size,
                total_rows,
                formatReadableSize(total_bytes/total_rows) as avg_row_size
            FROM system.tables
            WHERE database = currentDatabase()
            AND table IN ('{}', '{}')
            ORDER BY table
        "#, self.raw_table, self.latest_table);

        let result = self.client.query(&q).fetch_all::<String>().await?;
        Ok(format!("Table stats: {:?}", result))
    }
}

impl vixen::Handler<TokenBalanceOutput> for ClickHouseAsyncHandler {
    async fn handle(&self, value: &TokenBalanceOutput) -> vixen::HandlerResult<()> {
        if value.data.is_empty() {
            return Ok(());
        }

        let current_slot = value.data.first().map(|r| r.block_slot).unwrap_or(0);
        tracing::debug!("🔄 Queuing {} balance changes (slot: {})", value.data.len(), current_slot);

        // Send to background pipeline
        {
            let sender_lock = self.sender.read().await;
            if let Some(sender) = sender_lock.as_ref() {
                match sender.send(value.data.clone()) {
                    Ok(_) => return Ok(()),
                    Err(_) => {
                        tracing::warn!("⚠️  Background pipeline unavailable, falling back to sync");
                    }
                }
            }
        }

        // Fallback to direct insert (shouldn't happen in normal operation)
        if let Err(e) = Self::insert_batch(&self.client, &self.raw_table, &value.data).await {
            tracing::error!("❌ ClickHouse insert failed: {}", e);
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("ClickHouse insert failed: {}", e),
            )));
        }
        Ok(())
    }
}

fn now() -> i64 {
    UNIX_EPOCH.elapsed().expect("invalid system time").as_nanos() as i64
}