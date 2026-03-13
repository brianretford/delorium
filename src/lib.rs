use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use primitive_types::U256;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha3::{Digest, Keccak256};

#[derive(Debug)]
pub enum PipelineError {
    Io(std::io::Error),
    Toml(toml::de::Error),
    Config(String),
    Source(String),
}

impl Display for PipelineError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Toml(err) => write!(f, "toml error: {err}"),
            Self::Config(message) => write!(f, "config error: {message}"),
            Self::Source(message) => write!(f, "source error: {message}"),
        }
    }
}

impl std::error::Error for PipelineError {}

impl From<std::io::Error> for PipelineError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<toml::de::Error> for PipelineError {
    fn from(value: toml::de::Error) -> Self {
        Self::Toml(value)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PipelineConfig {
    pub name: String,
    pub version: u32,
    pub description: String,
    pub network: NetworkConfig,
    pub asset: AssetConfig,
    pub trigger: TriggerConfig,
    pub sources: HashMap<String, SourceConfig>,
    pub checkpoints: CheckpointConfig,
    pub mapper: MapperConfig,
    pub reducer: ReducerConfig,
    pub compute: ComputeConfig,
    pub materialization: MaterializationConfig,
    pub retention: RetentionConfig,
    pub destination: DestinationConfig,
    pub requestor: RequestorConfig,
    pub adapters: AdaptersConfig,
    #[serde(default)]
    pub stages: Vec<StageConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct StageConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub input: Option<String>,
    pub source: Option<String>,
    pub proof_boundary: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct NetworkConfig {
    pub chain: String,
    pub settlement_chain: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AssetConfig {
    pub base: String,
    pub quote: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TriggerConfig {
    pub mode: String,
    pub conditions: Vec<TriggerCondition>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TriggerCondition {
    #[serde(rename = "type")]
    pub kind: String,
    pub max_age_seconds: Option<u64>,
    pub threshold_bps: Option<u32>,
    pub reference: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub adapter: String,
    pub chain: String,
    pub pool_id: String,
    pub base_token: String,
    pub quote_token: String,
    pub base_token_address: Option<String>,
    pub quote_token_address: Option<String>,
    pub base_decimals: Option<u8>,
    pub quote_decimals: Option<u8>,
    pub quote_notionals: Vec<String>,
    pub normalization: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CheckpointConfig {
    pub schedule: String,
    pub interval_seconds: u64,
    pub count: u32,
    pub anchor: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct MapperConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub source: String,
    pub proof_boundary: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ReducerConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub input: String,
    pub window: String,
    pub flash_protection: String,
    pub outlier_policy: String,
    pub max_point_deviation_bps: u32,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ComputeConfig {
    pub execution: String,
    pub backend: String,
    pub guest: String,
    pub merklize: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct MaterializationConfig {
    pub batch_commitment: String,
    pub publish: Vec<String>,
    pub data_availability: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RetentionConfig {
    pub onchain_recent_reduced_outputs: u32,
    pub offchain_checkpoint_history: u32,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DestinationConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub adapter: String,
    pub output_key: String,
    pub decimals: u8,
    pub publish_mode: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RequestorConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub authorization: String,
    pub callback: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AdaptersConfig {
    pub source: HashMap<String, SourceAdapterConfig>,
    pub destination: HashMap<String, DestinationAdapterConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SourceAdapterConfig {
    pub kind: String,
    pub reads: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DestinationAdapterConfig {
    pub kind: String,
    pub contract: String,
    pub interface: String,
    pub stores: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub index: u32,
    pub label: String,
    pub timestamp_offset_seconds: u64,
    pub target_timestamp: u64,
    pub block_number: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MapperJob {
    pub map_key: String,
    pub job_id: String,
    pub source_id: String,
    pub pool_id: String,
    pub chain: String,
    pub quote_notional: String,
    pub checkpoint: Checkpoint,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionMode {
    DryRun,
    Prove,
}

impl ExecutionMode {
    pub fn from_str(value: &str) -> Result<Self, PipelineError> {
        match value {
            "dry-run" => Ok(Self::DryRun),
            "prove" => Ok(Self::Prove),
            other => Err(PipelineError::Config(format!(
                "unknown execution mode {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MapperOutput {
    pub job: MapperJob,
    pub observed_value: u128,
    pub proof_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MapperRun {
    pub pipeline_name: String,
    pub stage_id: String,
    pub mode: ExecutionMode,
    pub outputs: Vec<MapperOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointRun {
    pub pipeline_name: String,
    pub stage_id: String,
    pub checkpoints: Vec<ReducedCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistogramMapOutput {
    pub anchor_index: usize,
    pub checkpoint_label: String,
    pub block_number: Option<u64>,
    pub target_timestamp: u64,
    pub window_label: String,
    pub checkpoint_count: usize,
    pub checkpoint_value: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistogramMapRun {
    pub pipeline_name: String,
    pub stage_id: String,
    pub outputs: Vec<HistogramMapOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReducedCheckpoint {
    pub checkpoint_label: String,
    pub block_number: Option<u64>,
    pub target_timestamp: u64,
    pub notionals: Vec<String>,
    pub values: Vec<u128>,
    pub reduced_value: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReducerRun {
    pub pipeline_name: String,
    pub reducer_id: String,
    pub reducer_kind: String,
    pub checkpoints: Vec<ReducedCheckpoint>,
    pub histogram: Vec<ReducedAnchor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReducedWindow {
    pub label: String,
    pub checkpoint_count: usize,
    pub value: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReducedAnchor {
    pub checkpoint_label: String,
    pub block_number: Option<u64>,
    pub target_timestamp: u64,
    pub windows: Vec<ReducedWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelineStageResult {
    pub stage_id: String,
    pub kind: String,
    pub artifact_path: String,
    pub records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelineRunSummary {
    pub pipeline_name: String,
    pub mode: ExecutionMode,
    pub stages: Vec<PipelineStageResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MapperOptions {
    pub checkpoint_limit: Option<u32>,
}

pub trait PriceSource {
    fn fetch_price(&self, job: &MapperJob) -> Result<u128, PipelineError>;
}

pub trait CheckpointResolver {
    fn resolve(&self, config: &PipelineConfig) -> Result<Vec<Checkpoint>, PipelineError>;

    fn resolve_with_progress(
        &self,
        config: &PipelineConfig,
        on_progress: &mut dyn FnMut(usize, usize),
    ) -> Result<Vec<Checkpoint>, PipelineError> {
        let checkpoints = self.resolve(config)?;
        on_progress(checkpoints.len(), checkpoints.len());
        Ok(checkpoints)
    }
}

pub struct FixturePriceSource {
    values: HashMap<String, u128>,
}

impl FixturePriceSource {
    pub fn new(values: HashMap<String, u128>) -> Self {
        Self { values }
    }
}

impl PriceSource for FixturePriceSource {
    fn fetch_price(&self, job: &MapperJob) -> Result<u128, PipelineError> {
        let key = fixture_key(job);
        self.values
            .get(&key)
            .copied()
            .ok_or_else(|| PipelineError::Source(format!("missing fixture for {key}")))
    }
}

pub struct NoopPriceSource;

impl PriceSource for NoopPriceSource {
    fn fetch_price(&self, _job: &MapperJob) -> Result<u128, PipelineError> {
        Ok(0)
    }
}

pub struct AlchemySpotPriceSource {
    client: reqwest::blocking::Client,
    rpc_url: String,
    base_token_address: String,
    quote_token_address: String,
    base_decimals: u8,
    quote_decimals: u8,
}

impl AlchemySpotPriceSource {
    pub fn from_config(
        source: &SourceConfig,
        endpoint_or_key: &str,
    ) -> Result<Self, PipelineError> {
        let rpc_url = alchemy_rpc_url(endpoint_or_key, &source.chain)?;
        Ok(Self {
            client: reqwest::blocking::Client::builder()
                .build()
                .map_err(|err| PipelineError::Source(format!("failed to build http client: {err}")))?,
            rpc_url,
            base_token_address: source
                .base_token_address
                .clone()
                .ok_or_else(|| PipelineError::Config("missing base_token_address".to_string()))?,
            quote_token_address: source
                .quote_token_address
                .clone()
                .ok_or_else(|| PipelineError::Config("missing quote_token_address".to_string()))?,
            base_decimals: source
                .base_decimals
                .ok_or_else(|| PipelineError::Config("missing base_decimals".to_string()))?,
            quote_decimals: source
                .quote_decimals
                .ok_or_else(|| PipelineError::Config("missing quote_decimals".to_string()))?,
        })
    }

    fn rpc<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, PipelineError> {
        #[derive(Deserialize)]
        struct RpcResponse<T> {
            result: Option<T>,
            error: Option<RpcError>,
        }

        #[derive(Deserialize)]
        struct RpcError {
            message: String,
        }

        let response = self
            .client
            .post(&self.rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .map_err(|err| PipelineError::Source(format!("rpc {method} failed: {err}")))?;

        let status = response.status();
        if !status.is_success() {
            return Err(PipelineError::Source(format!(
                "rpc {method} returned http {status}"
            )));
        }

        let body = response
            .json::<RpcResponse<T>>()
            .map_err(|err| PipelineError::Source(format!("invalid rpc {method} response: {err}")))?;

        if let Some(error) = body.error {
            return Err(PipelineError::Source(format!(
                "rpc {method} error: {}",
                error.message
            )));
        }

        body.result.ok_or_else(|| {
            PipelineError::Source(format!("rpc {method} did not include a result"))
        })
    }
}

impl PriceSource for AlchemySpotPriceSource {
    fn fetch_price(&self, job: &MapperJob) -> Result<u128, PipelineError> {
        let block_number = job
            .checkpoint
            .block_number
            .ok_or_else(|| PipelineError::Source(format!("missing block for {}", job.map_key)))?;
        let state_view = state_view_address(&job.chain)?;
        let data = encode_get_slot0_call(&job.pool_id)?;
        let result: String = self.rpc(
            "eth_call",
            json!([
                {
                    "to": state_view,
                    "data": data,
                },
                format!("0x{block_number:x}")
            ]),
        )?;

        let sqrt_price_x96 = parse_sqrt_price_x96(&result)?;
        let price = price_e8_from_sqrt_price_x96(
            sqrt_price_x96,
            &self.base_token_address,
            self.base_decimals,
            &self.quote_token_address,
            self.quote_decimals,
        )?;
        Ok(price)
    }
}

pub struct CachedPriceSource<S> {
    inner: S,
    cache_path: PathBuf,
    cache: RefCell<QuoteCache>,
}

impl<S> CachedPriceSource<S> {
    pub fn new(inner: S, cache_name: &str) -> Self {
        let cache_path = PathBuf::from(".delorium-cache").join(format!("{cache_name}.json"));
        let cache = if let Ok(contents) = fs::read_to_string(&cache_path) {
            serde_json::from_str::<QuoteCache>(&contents).unwrap_or_default()
        } else {
            QuoteCache::default()
        };

        Self {
            inner,
            cache_path,
            cache: RefCell::new(cache),
        }
    }

    fn cache_key(job: &MapperJob) -> Result<String, PipelineError> {
        let locator = if let Some(block_number) = job.checkpoint.block_number {
            format!("block:{block_number}")
        } else {
            format!("timestamp:{}", job.checkpoint.target_timestamp)
        };
        Ok(format!(
            "source:{}:chain:{}:pool:{}:notional:{}@{}",
            job.source_id, job.chain, job.pool_id, job.quote_notional, locator
        ))
    }

    fn persist(&self) -> Result<(), PipelineError> {
        if let Some(parent) = self.cache_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_string_pretty(&*self.cache.borrow()).map_err(|err| {
            PipelineError::Source(format!("failed to serialize quote cache: {err}"))
        })?;
        fs::write(&self.cache_path, body)?;
        Ok(())
    }
}

impl<S> PriceSource for CachedPriceSource<S>
where
    S: PriceSource,
{
    fn fetch_price(&self, job: &MapperJob) -> Result<u128, PipelineError> {
        let key = Self::cache_key(job)?;
        if let Some(value) = self.cache.borrow().values.get(&key) {
            return Ok(*value);
        }

        let value = self.inner.fetch_price(job)?;
        self.cache.borrow_mut().values.insert(key, value);
        self.persist()?;
        Ok(value)
    }
}

fn fixture_key(job: &MapperJob) -> String {
    format!(
        "{}:{}:{}",
        job.source_id, job.checkpoint.label, job.quote_notional
    )
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct QuoteCache {
    values: HashMap<String, u128>,
}

pub struct StaticCheckpointResolver {
    pub anchor_timestamp: u64,
    pub latest_block_number: Option<u64>,
}

impl CheckpointResolver for StaticCheckpointResolver {
    fn resolve(&self, config: &PipelineConfig) -> Result<Vec<Checkpoint>, PipelineError> {
        Ok(build_checkpoints(
            config,
            self.anchor_timestamp,
            self.latest_block_number,
        ))
    }
}

pub struct AlchemyCheckpointResolver {
    client: reqwest::blocking::Client,
    chain: String,
    cache_path: PathBuf,
    rpc_url: String,
}

impl AlchemyCheckpointResolver {
    pub fn from_endpoint_or_key(endpoint_or_key: &str, chain: &str) -> Result<Self, PipelineError> {
        let rpc_url = alchemy_rpc_url(endpoint_or_key, chain)?;

        Ok(Self {
            client: reqwest::blocking::Client::builder()
                .build()
                .map_err(|err| PipelineError::Source(format!("failed to build http client: {err}")))?,
            chain: chain.to_string(),
            cache_path: PathBuf::from(".delorium-cache").join(format!("alchemy-{chain}.json")),
            rpc_url,
        })
    }

    fn rpc<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, PipelineError> {
        #[derive(Deserialize)]
        struct RpcResponse<T> {
            result: Option<T>,
            error: Option<RpcError>,
        }

        #[derive(Deserialize)]
        struct RpcError {
            message: String,
        }

        let response = self
            .client
            .post(&self.rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .map_err(|err| PipelineError::Source(format!("rpc {method} failed: {err}")))?;

        let status = response.status();
        if !status.is_success() {
            return Err(PipelineError::Source(format!(
                "rpc {method} returned http {status}"
            )));
        }

        let body = response
            .json::<RpcResponse<T>>()
            .map_err(|err| PipelineError::Source(format!("invalid rpc {method} response: {err}")))?;

        if let Some(error) = body.error {
            return Err(PipelineError::Source(format!(
                "rpc {method} error: {}",
                error.message
            )));
        }

        body.result.ok_or_else(|| {
            PipelineError::Source(format!("rpc {method} did not include a result"))
        })
    }

    fn latest_block(&self) -> Result<BlockHeader, PipelineError> {
        self.block_by_tag("latest")
    }

    fn block_by_tag(&self, tag: &str) -> Result<BlockHeader, PipelineError> {
        self.rpc("eth_getBlockByNumber", json!([tag, false]))
    }

    fn load_cache(&self) -> AlchemyCache {
        if let Ok(contents) = fs::read_to_string(&self.cache_path) {
            if let Ok(cache) = serde_json::from_str::<AlchemyCache>(&contents) {
                return cache;
            }
        }

        AlchemyCache {
            chain: self.chain.clone(),
            ..AlchemyCache::default()
        }
    }

    fn save_cache(&self, cache: &AlchemyCache) -> Result<(), PipelineError> {
        if let Some(parent) = self.cache_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let body = serde_json::to_string_pretty(cache).map_err(|err| {
            PipelineError::Source(format!("failed to serialize alchemy cache: {err}"))
        })?;
        fs::write(&self.cache_path, body)?;
        Ok(())
    }

    fn latest_block_cached(
        &self,
        cache: &mut AlchemyCache,
    ) -> Result<(u64, u64), PipelineError> {
        if let (Some(number), Some(timestamp)) = (cache.latest_block_number, cache.latest_timestamp) {
            return Ok((number, timestamp));
        }

        let latest = self.latest_block()?;
        let latest_number = parse_hex_u64(&latest.number)?;
        let latest_timestamp = parse_hex_u64(&latest.timestamp)?;
        cache.latest_block_number = Some(latest_number);
        cache.latest_timestamp = Some(latest_timestamp);
        cache.block_timestamps.insert(latest_number, latest_timestamp);
        Ok((latest_number, latest_timestamp))
    }

    fn find_block_at_or_before(
        &self,
        target_timestamp: u64,
        cache: &mut AlchemyCache,
    ) -> Result<u64, PipelineError> {
        if let Some(block_number) = cache.resolved_targets.get(&target_timestamp) {
            return Ok(*block_number);
        }

        let (latest_block_number, latest_timestamp) = self.latest_block_cached(cache)?;
        if target_timestamp >= latest_timestamp {
            cache.resolved_targets.insert(target_timestamp, latest_block_number);
            return Ok(latest_block_number);
        }

        let seconds_back = latest_timestamp.saturating_sub(target_timestamp);
        let avg_block_time = estimated_block_time_seconds(&self.chain);
        let blocks_back = ((seconds_back + avg_block_time.saturating_sub(1)) / avg_block_time)
            as u64;
        let block_number = latest_block_number.saturating_sub(blocks_back);
        cache.resolved_targets.insert(target_timestamp, block_number);
        Ok(block_number)
    }
}

fn alchemy_rpc_url(endpoint_or_key: &str, chain: &str) -> Result<String, PipelineError> {
    if endpoint_or_key.starts_with("http://") || endpoint_or_key.starts_with("https://") {
        return Ok(endpoint_or_key.to_string());
    }

    let network = alchemy_network_name(chain)?;
    Ok(format!("https://{network}.g.alchemy.com/v2/{endpoint_or_key}"))
}

fn alchemy_network_name(chain: &str) -> Result<&'static str, PipelineError> {
    match chain {
        "ethereum" => Ok("eth-mainnet"),
        "base" => Ok("base-mainnet"),
        other => Err(PipelineError::Config(format!(
            "unsupported alchemy chain {other}"
        ))),
    }
}

fn estimated_block_time_seconds(chain: &str) -> u64 {
    match chain {
        "ethereum" => 12,
        "base" => 2,
        _ => 12,
    }
}

fn state_view_address(chain: &str) -> Result<&'static str, PipelineError> {
    match chain {
        "ethereum" => Ok("0x7ffe42c4a5deea5b0fec41c94c136cf115597227"),
        "base" => Ok("0xa3c0c9b65bad0b08107aa264b0f3db444b867a71"),
        other => Err(PipelineError::Config(format!(
            "unsupported state view chain {other}"
        ))),
    }
}

fn encode_get_slot0_call(pool_id: &str) -> Result<String, PipelineError> {
    let pool_id = pool_id.trim_start_matches("0x");
    if pool_id.len() != 64 {
        return Err(PipelineError::Config(format!(
            "pool_id must be 32 bytes, got {} hex chars",
            pool_id.len()
        )));
    }
    let selector = &Keccak256::digest(b"getSlot0(bytes32)")[0..4];
    let mut data = String::from("0x");
    for byte in selector {
        data.push_str(&format!("{byte:02x}"));
    }
    data.push_str(pool_id);
    Ok(data)
}

fn parse_sqrt_price_x96(value: &str) -> Result<U256, PipelineError> {
    let bytes = hex_to_bytes(value)?;
    if bytes.len() < 32 {
        return Err(PipelineError::Source(
            "slot0 response shorter than one word".to_string(),
        ));
    }

    let first_word = &bytes[0..32];
    Ok(U256::from_big_endian(first_word))
}

fn price_e8_from_sqrt_price_x96(
    sqrt_price_x96: U256,
    base_token_address: &str,
    base_decimals: u8,
    quote_token_address: &str,
    quote_decimals: u8,
) -> Result<u128, PipelineError> {
    let base_is_token0 = normalize_address(base_token_address)? < normalize_address(quote_token_address)?;
    let ratio_x192 = sqrt_price_x96
        .checked_mul(sqrt_price_x96)
        .ok_or_else(|| PipelineError::Source("sqrt price overflow".to_string()))?;
    let q192 = U256::one() << 192;
    let price_x18 = if base_is_token0 {
        let scale = pow10_i32(18 + i32::from(base_decimals) - i32::from(quote_decimals))?;
        ratio_x192
            .checked_mul(scale)
            .ok_or_else(|| PipelineError::Source("price scale overflow".to_string()))?
            / q192
    } else {
        let scale = pow10_i32(18 + i32::from(quote_decimals) - i32::from(base_decimals))?;
        q192
            .checked_mul(scale)
            .ok_or_else(|| PipelineError::Source("inverse price scale overflow".to_string()))?
            / ratio_x192
    };

    Ok((price_x18 / U256::exp10(10)).as_u128())
}

fn pow10_i32(exp: i32) -> Result<U256, PipelineError> {
    if exp < 0 {
        return Err(PipelineError::Config(format!(
            "negative decimal exponent {exp}"
        )));
    }
    Ok(U256::exp10(exp as usize))
}

fn normalize_address(address: &str) -> Result<[u8; 20], PipelineError> {
    let bytes = hex_to_bytes(address)?;
    if bytes.len() != 20 {
        return Err(PipelineError::Config(format!(
            "address must be 20 bytes, got {}",
            bytes.len()
        )));
    }
    let mut out = [0_u8; 20];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn hex_to_bytes(value: &str) -> Result<Vec<u8>, PipelineError> {
    let value = value.trim_start_matches("0x");
    if value.len() % 2 != 0 {
        return Err(PipelineError::Source(format!(
            "hex value has odd length: {}",
            value.len()
        )));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let chars: Vec<char> = value.chars().collect();
    for i in (0..chars.len()).step_by(2) {
        let hi = chars[i]
            .to_digit(16)
            .ok_or_else(|| PipelineError::Source(format!("invalid hex char {}", chars[i])))?;
        let lo = chars[i + 1]
            .to_digit(16)
            .ok_or_else(|| PipelineError::Source(format!("invalid hex char {}", chars[i + 1])))?;
        bytes.push(((hi << 4) | lo) as u8);
    }
    Ok(bytes)
}

impl CheckpointResolver for AlchemyCheckpointResolver {
    fn resolve(&self, config: &PipelineConfig) -> Result<Vec<Checkpoint>, PipelineError> {
        self.resolve_with_progress(config, &mut |_completed, _total| {})
    }

    fn resolve_with_progress(
        &self,
        config: &PipelineConfig,
        on_progress: &mut dyn FnMut(usize, usize),
    ) -> Result<Vec<Checkpoint>, PipelineError> {
        let mut cache = self.load_cache();
        let (latest_block_number, anchor_timestamp) = self.latest_block_cached(&mut cache)?;
        let mut checkpoints = build_checkpoints(config, anchor_timestamp, Some(latest_block_number));
        let total = checkpoints.len();
        on_progress(0, total);
        for (index, checkpoint) in checkpoints.iter_mut().enumerate() {
            checkpoint.block_number =
                Some(self.find_block_at_or_before(checkpoint.target_timestamp, &mut cache)?);
            on_progress(index + 1, total);
        }
        self.save_cache(&cache)?;
        Ok(checkpoints)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AlchemyCache {
    chain: String,
    latest_block_number: Option<u64>,
    latest_timestamp: Option<u64>,
    block_timestamps: HashMap<u64, u64>,
    resolved_targets: HashMap<u64, u64>,
}

#[derive(Debug, Deserialize)]
struct BlockHeader {
    number: String,
    timestamp: String,
}

fn parse_hex_u64(value: &str) -> Result<u64, PipelineError> {
    u64::from_str_radix(value.trim_start_matches("0x"), 16).map_err(|err| {
        PipelineError::Source(format!("failed to parse hex value {value}: {err}"))
    })
}

pub fn load_pipeline(path: impl AsRef<Path>) -> Result<PipelineConfig, PipelineError> {
    let content = fs::read_to_string(path)?;
    let config = toml::from_str::<PipelineConfig>(&content)?;
    validate_pipeline(&config)?;
    Ok(config)
}

pub fn load_mapper_run(path: impl AsRef<Path>) -> Result<MapperRun, PipelineError> {
    let content = fs::read_to_string(path)?;
    serde_json::from_str::<MapperRun>(&content)
        .map_err(|err| PipelineError::Source(format!("invalid mapper run json: {err}")))
}

pub fn load_checkpoint_run(path: impl AsRef<Path>) -> Result<CheckpointRun, PipelineError> {
    let content = fs::read_to_string(path)?;
    serde_json::from_str::<CheckpointRun>(&content)
        .map_err(|err| PipelineError::Source(format!("invalid checkpoint run json: {err}")))
}

pub fn load_histogram_map_run(path: impl AsRef<Path>) -> Result<HistogramMapRun, PipelineError> {
    let content = fs::read_to_string(path)?;
    serde_json::from_str::<HistogramMapRun>(&content)
        .map_err(|err| PipelineError::Source(format!("invalid histogram map run json: {err}")))
}

pub fn load_reducer_run(path: impl AsRef<Path>) -> Result<ReducerRun, PipelineError> {
    let content = fs::read_to_string(path)?;
    serde_json::from_str::<ReducerRun>(&content)
        .map_err(|err| PipelineError::Source(format!("invalid reducer run json: {err}")))
}

pub fn write_mapper_run(path: impl AsRef<Path>, run: &MapperRun) -> Result<(), PipelineError> {
    let body = serde_json::to_string_pretty(run)
        .map_err(|err| PipelineError::Source(format!("failed to serialize mapper run: {err}")))?;
    fs::write(path, body)?;
    Ok(())
}

pub fn write_checkpoint_run(
    path: impl AsRef<Path>,
    run: &CheckpointRun,
) -> Result<(), PipelineError> {
    let body = serde_json::to_string_pretty(run).map_err(|err| {
        PipelineError::Source(format!("failed to serialize checkpoint run: {err}"))
    })?;
    fs::write(path, body)?;
    Ok(())
}

pub fn write_histogram_map_run(
    path: impl AsRef<Path>,
    run: &HistogramMapRun,
) -> Result<(), PipelineError> {
    let body = serde_json::to_string_pretty(run).map_err(|err| {
        PipelineError::Source(format!("failed to serialize histogram map run: {err}"))
    })?;
    fs::write(path, body)?;
    Ok(())
}

pub fn write_reducer_run(path: impl AsRef<Path>, run: &ReducerRun) -> Result<(), PipelineError> {
    let body = serde_json::to_string_pretty(run)
        .map_err(|err| PipelineError::Source(format!("failed to serialize reducer run: {err}")))?;
    fs::write(path, body)?;
    Ok(())
}

pub fn write_chart_html(path: impl AsRef<Path>, body: &str) -> Result<(), PipelineError> {
    fs::write(path, body)?;
    Ok(())
}

fn validate_pipeline(config: &PipelineConfig) -> Result<(), PipelineError> {
    if config.checkpoints.count == 0 {
        return Err(PipelineError::Config(
            "checkpoints.count must be greater than zero".to_string(),
        ));
    }
    if config.checkpoints.interval_seconds == 0 {
        return Err(PipelineError::Config(
            "checkpoints.interval_seconds must be greater than zero".to_string(),
        ));
    }
    let source_id = if let Some(stage) = config
        .stages
        .iter()
        .find(|stage| stage.kind == "map_checkpoints")
    {
        stage
            .source
            .as_ref()
            .ok_or_else(|| PipelineError::Config("map_checkpoints stage missing source".to_string()))?
            .clone()
    } else {
        config.mapper.source.clone()
    };
    let source = config
        .sources
        .get(&source_id)
        .ok_or_else(|| PipelineError::Config(format!("unknown mapper source {source_id}")))?;
    if source.quote_notionals.is_empty() {
        return Err(PipelineError::Config(
            "source quote_notionals must not be empty".to_string(),
        ));
    }
    if !config.adapters.source.contains_key(&source.adapter) {
        return Err(PipelineError::Config(format!(
            "missing source adapter config {}",
            source.adapter
        )));
    }
    if !config
        .adapters
        .destination
        .contains_key(&config.destination.adapter)
    {
        return Err(PipelineError::Config(format!(
            "missing destination adapter config {}",
            config.destination.adapter
        )));
    }

    if !config.stages.is_empty() {
        let mut seen = HashMap::new();
        for (index, stage) in config.stages.iter().enumerate() {
            if seen.insert(stage.id.clone(), index).is_some() {
                return Err(PipelineError::Config(format!(
                    "duplicate stage id {}",
                    stage.id
                )));
            }
            match stage.kind.as_str() {
                "map_checkpoints" => {
                    let source_id = stage.source.as_ref().ok_or_else(|| {
                        PipelineError::Config(format!("stage {} missing source", stage.id))
                    })?;
                    if !config.sources.contains_key(source_id) {
                        return Err(PipelineError::Config(format!(
                            "stage {} references unknown source {}",
                            stage.id, source_id
                        )));
                    }
                }
                "reduce_checkpoint_price" | "map_histogram" | "reduce_histogram" => {
                    let input = stage.input.as_ref().ok_or_else(|| {
                        PipelineError::Config(format!("stage {} missing input", stage.id))
                    })?;
                    if !seen.contains_key(input) {
                        return Err(PipelineError::Config(format!(
                            "stage {} references unknown prior input {}",
                            stage.id, input
                        )));
                    }
                }
                other => {
                    return Err(PipelineError::Config(format!(
                        "unsupported stage type {}",
                        other
                    )));
                }
            }
        }
    }
    Ok(())
}

pub fn pipeline_stages(config: &PipelineConfig) -> Vec<StageConfig> {
    if !config.stages.is_empty() {
        return config.stages.clone();
    }

    vec![
        StageConfig {
            id: config.mapper.id.clone(),
            kind: "map_checkpoints".to_string(),
            input: None,
            source: Some(config.mapper.source.clone()),
            proof_boundary: Some(config.mapper.proof_boundary.clone()),
        },
        StageConfig {
            id: "reduce-checkpoint-price".to_string(),
            kind: "reduce_checkpoint_price".to_string(),
            input: Some(config.mapper.id.clone()),
            source: None,
            proof_boundary: None,
        },
        StageConfig {
            id: "map-histogram".to_string(),
            kind: "map_histogram".to_string(),
            input: Some("reduce-checkpoint-price".to_string()),
            source: None,
            proof_boundary: None,
        },
        StageConfig {
            id: config.reducer.id.clone(),
            kind: "reduce_histogram".to_string(),
            input: Some("map-histogram".to_string()),
            source: None,
            proof_boundary: None,
        },
    ]
}

pub fn expand_mapper_jobs(
    config: &PipelineConfig,
    checkpoints: &[Checkpoint],
) -> Result<Vec<MapperJob>, PipelineError> {
    let source = config
        .sources
        .get(&config.mapper.source)
        .ok_or_else(|| PipelineError::Config(format!("unknown mapper source {}", config.mapper.source)))?;

    let mut jobs = Vec::new();
    for checkpoint in checkpoints {
        for quote_notional in &source.quote_notionals {
            let map_key = format!(
                "{}:{}:{}:{}",
                config.name,
                config.mapper.source,
                checkpoint.label,
                quote_notional
            );
            let job_id = format!("{}:{}", config.mapper.id, map_key);
            jobs.push(MapperJob {
                map_key,
                job_id,
                source_id: config.mapper.source.clone(),
                pool_id: source.pool_id.clone(),
                chain: source.chain.clone(),
                quote_notional: quote_notional.clone(),
                checkpoint: checkpoint.clone(),
            });
        }
    }

    Ok(jobs)
}

pub fn run_mapper(
    config: &PipelineConfig,
    mode: ExecutionMode,
    options: MapperOptions,
    checkpoint_resolver: &dyn CheckpointResolver,
    source: &dyn PriceSource,
) -> Result<MapperRun, PipelineError> {
    run_mapper_stage(
        config,
        &config.mapper.id,
        mode,
        options,
        checkpoint_resolver,
        source,
    )
}

pub fn run_mapper_stage(
    config: &PipelineConfig,
    stage_id: &str,
    mode: ExecutionMode,
    options: MapperOptions,
    checkpoint_resolver: &dyn CheckpointResolver,
    source: &dyn PriceSource,
) -> Result<MapperRun, PipelineError> {
    run_mapper_stage_with_progress(
        config,
        stage_id,
        mode,
        options,
        checkpoint_resolver,
        source,
        &mut |_completed, _total| {},
        |_completed, _total, _job| {},
    )
}

pub fn run_mapper_stage_with_progress<F>(
    config: &PipelineConfig,
    stage_id: &str,
    mode: ExecutionMode,
    options: MapperOptions,
    checkpoint_resolver: &dyn CheckpointResolver,
    source: &dyn PriceSource,
    mut on_resolve_progress: &mut dyn FnMut(usize, usize),
    mut on_progress: F,
) -> Result<MapperRun, PipelineError>
where
    F: FnMut(usize, usize, &MapperJob),
{
    let mut checkpoints = checkpoint_resolver.resolve_with_progress(config, &mut on_resolve_progress)?;
    if let Some(limit) = options.checkpoint_limit {
        checkpoints.truncate(limit as usize);
    }
    let jobs = expand_mapper_jobs(config, &checkpoints)?;
    let total = jobs.len();
    let outputs = jobs
        .into_iter()
        .enumerate()
        .map(|(index, job)| {
            let observed_value = source.fetch_price(&job)?;
            let proof_id = match mode {
                ExecutionMode::DryRun => None,
                ExecutionMode::Prove => Some(format!("proof:{}", job.job_id)),
            };
            on_progress(index + 1, total, &job);
            Ok(MapperOutput {
                job,
                observed_value,
                proof_id,
            })
        })
        .collect::<Result<Vec<_>, PipelineError>>()?;

    Ok(MapperRun {
        pipeline_name: config.name.clone(),
        stage_id: stage_id.to_string(),
        mode,
        outputs,
    })
}

pub fn summarize_mapper_run(run: &MapperRun) -> String {
    let first = run.outputs.first();
    let last = run.outputs.last();
    let proof_count = run
        .outputs
        .iter()
        .filter(|output| output.proof_id.is_some())
        .count();

    let first_label = first
        .map(|output| output.job.checkpoint.label.as_str())
        .unwrap_or("n/a");
    let last_label = last
        .map(|output| output.job.checkpoint.label.as_str())
        .unwrap_or("n/a");

    format!(
        "pipeline={} mode={:?} jobs={} proofs={} checkpoints={}..{}",
        run.pipeline_name,
        run.mode,
        run.outputs.len(),
        proof_count,
        first_label,
        last_label
    )
}

pub fn run_reducer(
    config: &PipelineConfig,
    mapper_run: &MapperRun,
) -> Result<ReducerRun, PipelineError> {
    let checkpoint_run = run_checkpoint_reducer(config, "reduce-checkpoint-price", mapper_run)?;
    let histogram_run = run_histogram_mapper(config, "map-histogram", &checkpoint_run)?;
    run_histogram_reducer(config, &config.reducer.id, &checkpoint_run, &histogram_run)
}

pub fn run_checkpoint_reducer(
    config: &PipelineConfig,
    stage_id: &str,
    mapper_run: &MapperRun,
) -> Result<CheckpointRun, PipelineError> {
    if mapper_run.outputs.is_empty() {
        return Err(PipelineError::Config(
            "mapper run must contain at least one observation".to_string(),
        ));
    }
    if mapper_run.pipeline_name != config.name {
        return Err(PipelineError::Config(format!(
            "mapper run pipeline {} does not match config {}",
            mapper_run.pipeline_name, config.name
        )));
    }

    let mut grouped: HashMap<String, Vec<&MapperOutput>> = HashMap::new();
    for output in &mapper_run.outputs {
        grouped
            .entry(output.job.checkpoint.label.clone())
            .or_default()
            .push(output);
    }

    let mut checkpoints = grouped
        .into_iter()
        .map(|(label, outputs)| {
            let first = outputs[0];
            let mut values: Vec<u128> = outputs.iter().map(|output| output.observed_value).collect();
            values.sort_unstable();
            let reduced_value = match config.reducer.kind.as_str() {
                "twap" => median(&values),
                "median" => median(&values),
                other => {
                    return Err(PipelineError::Config(format!(
                        "unsupported reducer type {other}"
                    )))
                }
            };

            let notionals = outputs
                .iter()
                .map(|output| output.job.quote_notional.clone())
                .collect::<Vec<_>>();

            Ok(ReducedCheckpoint {
                checkpoint_label: label,
                block_number: first.job.checkpoint.block_number,
                target_timestamp: first.job.checkpoint.target_timestamp,
                notionals,
                values,
                reduced_value,
            })
        })
        .collect::<Result<Vec<_>, PipelineError>>()?;

    checkpoints.sort_by_key(|checkpoint| checkpoint.target_timestamp);
    Ok(CheckpointRun {
        pipeline_name: config.name.clone(),
        stage_id: stage_id.to_string(),
        checkpoints,
    })
}

pub fn run_histogram_mapper(
    config: &PipelineConfig,
    stage_id: &str,
    checkpoint_run: &CheckpointRun,
) -> Result<HistogramMapRun, PipelineError> {
    if checkpoint_run.pipeline_name != config.name {
        return Err(PipelineError::Config(format!(
            "checkpoint run pipeline {} does not match config {}",
            checkpoint_run.pipeline_name, config.name
        )));
    }

    let outputs = map_histogram_contributions(
        &checkpoint_run.checkpoints,
        config.checkpoints.interval_seconds,
        &window_specs(),
    );

    Ok(HistogramMapRun {
        pipeline_name: config.name.clone(),
        stage_id: stage_id.to_string(),
        outputs,
    })
}

pub fn run_histogram_reducer(
    config: &PipelineConfig,
    stage_id: &str,
    checkpoint_run: &CheckpointRun,
    histogram_run: &HistogramMapRun,
) -> Result<ReducerRun, PipelineError> {
    if checkpoint_run.pipeline_name != config.name {
        return Err(PipelineError::Config(format!(
            "checkpoint run pipeline {} does not match config {}",
            checkpoint_run.pipeline_name, config.name
        )));
    }
    if histogram_run.pipeline_name != config.name {
        return Err(PipelineError::Config(format!(
            "histogram run pipeline {} does not match config {}",
            histogram_run.pipeline_name, config.name
        )));
    }

    let histogram = match config.reducer.kind.as_str() {
        "twap" => reduce_histogram_contributions(
            &checkpoint_run.checkpoints,
            &window_specs(),
            &histogram_run.outputs,
        )?,
        "median" => checkpoint_run
            .checkpoints
            .iter()
            .map(|checkpoint| ReducedAnchor {
                checkpoint_label: checkpoint.checkpoint_label.clone(),
                block_number: checkpoint.block_number,
                target_timestamp: checkpoint.target_timestamp,
                windows: vec![ReducedWindow {
                    label: "median".to_string(),
                    checkpoint_count: 1,
                    value: checkpoint.reduced_value,
                }],
            })
            .collect(),
        other => {
            return Err(PipelineError::Config(format!(
                "unsupported reducer type {other}"
            )))
        }
    };

    Ok(ReducerRun {
        pipeline_name: config.name.clone(),
        reducer_id: stage_id.to_string(),
        reducer_kind: config.reducer.kind.clone(),
        checkpoints: checkpoint_run.checkpoints.clone(),
        histogram,
    })
}

pub fn summarize_reducer_run(run: &ReducerRun) -> String {
    let first = run
        .histogram
        .first()
        .map(|anchor| anchor.checkpoint_label.as_str())
        .unwrap_or("n/a");
    let last = run
        .histogram
        .last()
        .map(|anchor| anchor.checkpoint_label.as_str())
        .unwrap_or("n/a");

    format!(
        "pipeline={} reducer={} kind={} checkpoints={} anchors={}..{}",
        run.pipeline_name,
        run.reducer_id,
        run.reducer_kind,
        run.checkpoints.len(),
        first,
        last
    )
}

pub fn summarize_pipeline_run(run: &PipelineRunSummary) -> String {
    format!(
        "pipeline={} mode={:?} stages={}",
        run.pipeline_name,
        run.mode,
        run.stages.len()
    )
}

pub fn render_histogram_chart(
    run: &ReducerRun,
    quote_decimals: u8,
) -> Result<String, PipelineError> {
    if run.histogram.is_empty() {
        return Err(PipelineError::Config(
            "reducer run histogram must not be empty".to_string(),
        ));
    }

    let mut labels = Vec::new();
    let mut series_order = Vec::new();
    let mut series_data: HashMap<String, Vec<Option<u128>>> = HashMap::new();

    for (anchor_index, anchor) in run.histogram.iter().enumerate() {
        labels.push(format_timestamp(anchor.target_timestamp));
        for values in series_data.values_mut() {
            values.push(None);
        }
        for window in &anchor.windows {
            if !series_data.contains_key(&window.label) {
                series_order.push(window.label.clone());
                series_data.insert(window.label.clone(), vec![None; anchor_index + 1]);
            }
            if let Some(values) = series_data.get_mut(&window.label) {
                if values.len() < anchor_index + 1 {
                    values.resize(anchor_index + 1, None);
                }
                values[anchor_index] = Some(window.value);
            }
        }
    }

    let all_values = series_order
        .iter()
        .filter_map(|label| series_data.get(label))
        .flat_map(|values| values.iter().flatten().copied())
        .collect::<Vec<_>>();
    let min_value = all_values
        .iter()
        .min()
        .copied()
        .ok_or_else(|| PipelineError::Config("chart data must not be empty".to_string()))?;
    let max_value = all_values.iter().max().copied().unwrap_or(min_value);
    let y_min = if min_value == max_value {
        min_value.saturating_sub(1)
    } else {
        min_value
    };
    let y_max = if min_value == max_value {
        max_value.saturating_add(1)
    } else {
        max_value
    };

    let colors = [
        "#0b84f3", "#f39c12", "#27ae60", "#e74c3c", "#8e44ad", "#16a085", "#2c3e50",
    ];
    let chart_width = 1100.0_f64;
    let chart_height = 520.0_f64;
    let margin_left = 72.0_f64;
    let margin_right = 24.0_f64;
    let margin_top = 32.0_f64;
    let margin_bottom = 72.0_f64;
    let plot_width = chart_width - margin_left - margin_right;
    let plot_height = chart_height - margin_top - margin_bottom;

    let denom_x = (labels.len().saturating_sub(1)).max(1) as f64;
    let y_span = (y_max - y_min).max(1) as f64;

    let mut polylines = Vec::new();
    let mut legend = Vec::new();
    for (index, label) in series_order.iter().enumerate() {
        let color = colors[index % colors.len()];
        let Some(values) = series_data.get(label) else {
            continue;
        };
        let points = values
            .iter()
            .enumerate()
            .filter_map(|(i, value)| {
                value.map(|value| {
                    let x = margin_left + (i as f64 / denom_x) * plot_width;
                    let y =
                        margin_top + ((y_max - value) as f64 / y_span) * plot_height;
                    format!("{x:.2},{y:.2}")
                })
            })
            .collect::<Vec<_>>();
        if points.len() >= 2 {
            polylines.push(format!(
                r#"<polyline fill="none" stroke="{color}" stroke-width="2.5" points="{}" />"#,
                points.join(" ")
            ));
        } else if points.len() == 1 {
            polylines.push(format!(
                r#"<circle cx="{}" cy="{}" r="3" fill="{color}" />"#,
                points[0].split(',').next().unwrap_or("0"),
                points[0].split(',').nth(1).unwrap_or("0")
            ));
        }
        legend.push(format!(
            r#"<div class="legend-item"><span class="legend-swatch" style="background:{color}"></span><span>{label}</span></div>"#
        ));
    }

    let y_ticks = 5;
    let mut grid_lines = Vec::new();
    let mut y_labels = Vec::new();
    for tick in 0..=y_ticks {
        let ratio = tick as f64 / y_ticks as f64;
        let y = margin_top + ratio * plot_height;
        let value = y_max as f64 - ratio * y_span;
        grid_lines.push(format!(
            r#"<line x1="{margin_left:.2}" y1="{y:.2}" x2="{:.2}" y2="{y:.2}" class="grid" />"#,
            chart_width - margin_right
        ));
        y_labels.push(format!(
            r#"<text x="{:.2}" y="{:.2}" class="axis-label" text-anchor="end">${}</text>"#,
            margin_left - 10.0,
            y + 4.0,
            format_fixed_decimals(value.round() as u128, quote_decimals)
        ));
    }

    let label_step = (labels.len() / 8).max(1);
    let mut x_labels = Vec::new();
    for (index, label) in labels.iter().enumerate().step_by(label_step) {
        let x = margin_left + (index as f64 / denom_x) * plot_width;
        x_labels.push(format!(
            r#"<text x="{x:.2}" y="{:.2}" class="axis-label" text-anchor="middle">{label}</text>"#,
            chart_height - margin_bottom + 24.0
        ));
    }

    Ok(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f5efe5;
      --panel: #fffaf2;
      --ink: #1f2933;
      --muted: #6b7280;
      --grid: #d6d3d1;
      --axis: #9ca3af;
    }}
    body {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      background:
        radial-gradient(circle at top left, rgba(243, 156, 18, 0.18), transparent 30%),
        linear-gradient(180deg, var(--bg), #efe7da);
      color: var(--ink);
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 24px 40px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 32px;
      line-height: 1.1;
    }}
    p {{
      margin: 0 0 20px;
      color: var(--muted);
      font-size: 15px;
    }}
    .panel {{
      background: rgba(255, 250, 242, 0.92);
      border: 1px solid rgba(31, 41, 51, 0.08);
      border-radius: 20px;
      box-shadow: 0 20px 50px rgba(31, 41, 51, 0.08);
      padding: 18px 18px 12px;
      overflow-x: auto;
    }}
    svg {{
      width: 100%;
      height: auto;
      min-width: 960px;
      display: block;
    }}
    .grid {{
      stroke: var(--grid);
      stroke-width: 1;
    }}
    .axis {{
      stroke: var(--axis);
      stroke-width: 1.2;
    }}
    .axis-label {{
      fill: var(--muted);
      font-size: 12px;
    }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px 18px;
      margin-top: 16px;
    }}
    .legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      color: var(--ink);
    }}
    .legend-swatch {{
      width: 14px;
      height: 14px;
      border-radius: 999px;
      display: inline-block;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>{title}</h1>
    <p>Each TWAP bin is rendered as its own series across checkpoint timestamps. Prices are shown as decimal dollar values.</p>
    <div class="panel">
      <svg viewBox="0 0 {chart_width} {chart_height}" role="img" aria-label="{title}">
        {grid_lines}
        <line x1="{margin_left:.2}" y1="{margin_top:.2}" x2="{margin_left:.2}" y2="{:.2}" class="axis" />
        <line x1="{margin_left:.2}" y1="{:.2}" x2="{:.2}" y2="{:.2}" class="axis" />
        {y_labels}
        {x_labels}
        {polylines}
      </svg>
      <div class="legend">{legend}</div>
    </div>
  </div>
</body>
</html>"#,
        chart_width - margin_right,
        chart_height - margin_bottom,
        chart_height - margin_bottom,
        chart_width - margin_right,
        title = format!("{} Histogram", run.pipeline_name),
        chart_width = chart_width,
        chart_height = chart_height,
        margin_left = margin_left,
        margin_top = margin_top,
        grid_lines = grid_lines.join(""),
        y_labels = y_labels.join(""),
        x_labels = x_labels.join(""),
        polylines = polylines.join(""),
        legend = legend.join("")
    ))
}

fn format_timestamp(timestamp: u64) -> String {
    let seconds = i64::try_from(timestamp).unwrap_or(i64::MAX);
    let Some(datetime) = DateTime::<Utc>::from_timestamp(seconds, 0) else {
        return timestamp.to_string();
    };
    datetime.format("%m-%d %H:%M").to_string()
}

fn format_fixed_decimals(value: u128, decimals: u8) -> String {
    if decimals == 0 {
        return value.to_string();
    }

    let scale = 10_u128.saturating_pow(decimals as u32);
    let whole = value / scale;
    let fractional = value % scale;
    format!(
        "{whole}.{:0width$}",
        fractional,
        width = decimals as usize
    )
}

pub fn stage_artifact_path(base_dir: impl AsRef<Path>, ordinal: usize, stage: &StageConfig) -> PathBuf {
    let extension = match stage.kind.as_str() {
        "map_checkpoints" => "mapper.json",
        "reduce_checkpoint_price" => "checkpoint.json",
        "map_histogram" => "histogram-map.json",
        "reduce_histogram" => "histogram.json",
        _ => "json",
    };
    base_dir
        .as_ref()
        .join(format!("{:02}-{}.{}", ordinal + 1, stage.id, extension))
}

pub fn run_pipeline(
    config: &PipelineConfig,
    mode: ExecutionMode,
    options: MapperOptions,
    checkpoint_resolver: &dyn CheckpointResolver,
    source: &dyn PriceSource,
    output_dir: impl AsRef<Path>,
) -> Result<PipelineRunSummary, PipelineError> {
    fs::create_dir_all(output_dir.as_ref())?;
    let stages = pipeline_stages(config);
    let mut results = Vec::with_capacity(stages.len());
    let mut mapper_run: Option<MapperRun> = None;
    let mut checkpoint_run: Option<CheckpointRun> = None;
    let mut histogram_map_run: Option<HistogramMapRun> = None;

    for (ordinal, stage) in stages.iter().enumerate() {
        let artifact_path = stage_artifact_path(output_dir.as_ref(), ordinal, stage);
        match stage.kind.as_str() {
            "map_checkpoints" => {
                let run = run_mapper_stage(
                    config,
                    &stage.id,
                    mode,
                    options,
                    checkpoint_resolver,
                    source,
                )?;
                write_mapper_run(&artifact_path, &run)?;
                results.push(PipelineStageResult {
                    stage_id: stage.id.clone(),
                    kind: stage.kind.clone(),
                    artifact_path: artifact_path.display().to_string(),
                    records: run.outputs.len(),
                });
                mapper_run = Some(run);
            }
            "reduce_checkpoint_price" => {
                let input = mapper_run.as_ref().ok_or_else(|| {
                    PipelineError::Config(format!("stage {} missing mapper input", stage.id))
                })?;
                let run = run_checkpoint_reducer(config, &stage.id, input)?;
                write_checkpoint_run(&artifact_path, &run)?;
                results.push(PipelineStageResult {
                    stage_id: stage.id.clone(),
                    kind: stage.kind.clone(),
                    artifact_path: artifact_path.display().to_string(),
                    records: run.checkpoints.len(),
                });
                checkpoint_run = Some(run);
            }
            "map_histogram" => {
                let input = checkpoint_run.as_ref().ok_or_else(|| {
                    PipelineError::Config(format!("stage {} missing checkpoint input", stage.id))
                })?;
                let run = run_histogram_mapper(config, &stage.id, input)?;
                write_histogram_map_run(&artifact_path, &run)?;
                results.push(PipelineStageResult {
                    stage_id: stage.id.clone(),
                    kind: stage.kind.clone(),
                    artifact_path: artifact_path.display().to_string(),
                    records: run.outputs.len(),
                });
                histogram_map_run = Some(run);
            }
            "reduce_histogram" => {
                let checkpoint_input = checkpoint_run.as_ref().ok_or_else(|| {
                    PipelineError::Config(format!("stage {} missing checkpoint input", stage.id))
                })?;
                let histogram_input = histogram_map_run.as_ref().ok_or_else(|| {
                    PipelineError::Config(format!("stage {} missing histogram map input", stage.id))
                })?;
                let run =
                    run_histogram_reducer(config, &stage.id, checkpoint_input, histogram_input)?;
                write_reducer_run(&artifact_path, &run)?;
                results.push(PipelineStageResult {
                    stage_id: stage.id.clone(),
                    kind: stage.kind.clone(),
                    artifact_path: artifact_path.display().to_string(),
                    records: run.histogram.len(),
                });
            }
            other => {
                return Err(PipelineError::Config(format!(
                    "unsupported stage type {}",
                    other
                )));
            }
        }
    }

    Ok(PipelineRunSummary {
        pipeline_name: config.name.clone(),
        mode,
        stages: results,
    })
}

pub fn run_pipeline_stage(
    config: &PipelineConfig,
    stage_id: &str,
    mode: ExecutionMode,
    options: MapperOptions,
    checkpoint_resolver: &dyn CheckpointResolver,
    source: &dyn PriceSource,
    output_dir: impl AsRef<Path>,
) -> Result<PipelineStageResult, PipelineError> {
    fs::create_dir_all(output_dir.as_ref())?;
    let stages = pipeline_stages(config);
    let stage_index = stages
        .iter()
        .position(|stage| stage.id == stage_id)
        .ok_or_else(|| PipelineError::Config(format!("unknown stage {}", stage_id)))?;
    let stage = &stages[stage_index];
    let artifact_path = stage_artifact_path(output_dir.as_ref(), stage_index, stage);

    let result = match stage.kind.as_str() {
        "map_checkpoints" => {
            let run =
                run_mapper_stage(config, &stage.id, mode, options, checkpoint_resolver, source)?;
            write_mapper_run(&artifact_path, &run)?;
            PipelineStageResult {
                stage_id: stage.id.clone(),
                kind: stage.kind.clone(),
                artifact_path: artifact_path.display().to_string(),
                records: run.outputs.len(),
            }
        }
        "reduce_checkpoint_price" => {
            let input_stage_index = stage_index.checked_sub(1).ok_or_else(|| {
                PipelineError::Config(format!("stage {} has no prior input", stage.id))
            })?;
            let input_stage = &stages[input_stage_index];
            let input_path = stage_artifact_path(output_dir.as_ref(), input_stage_index, input_stage);
            let mapper_run = load_mapper_run(&input_path)?;
            let run = run_checkpoint_reducer(config, &stage.id, &mapper_run)?;
            write_checkpoint_run(&artifact_path, &run)?;
            PipelineStageResult {
                stage_id: stage.id.clone(),
                kind: stage.kind.clone(),
                artifact_path: artifact_path.display().to_string(),
                records: run.checkpoints.len(),
            }
        }
        "map_histogram" => {
            let input_stage_index = stage_index.checked_sub(1).ok_or_else(|| {
                PipelineError::Config(format!("stage {} has no prior input", stage.id))
            })?;
            let input_stage = &stages[input_stage_index];
            let input_path = stage_artifact_path(output_dir.as_ref(), input_stage_index, input_stage);
            let checkpoint_run = load_checkpoint_run(&input_path)?;
            let run = run_histogram_mapper(config, &stage.id, &checkpoint_run)?;
            write_histogram_map_run(&artifact_path, &run)?;
            PipelineStageResult {
                stage_id: stage.id.clone(),
                kind: stage.kind.clone(),
                artifact_path: artifact_path.display().to_string(),
                records: run.outputs.len(),
            }
        }
        "reduce_histogram" => {
            let checkpoint_index = stage_index.checked_sub(2).ok_or_else(|| {
                PipelineError::Config(format!("stage {} missing checkpoint input", stage.id))
            })?;
            let histogram_index = stage_index.checked_sub(1).ok_or_else(|| {
                PipelineError::Config(format!("stage {} missing histogram input", stage.id))
            })?;
            let checkpoint_path =
                stage_artifact_path(output_dir.as_ref(), checkpoint_index, &stages[checkpoint_index]);
            let histogram_path =
                stage_artifact_path(output_dir.as_ref(), histogram_index, &stages[histogram_index]);
            let checkpoint_run = load_checkpoint_run(&checkpoint_path)?;
            let histogram_run = load_histogram_map_run(&histogram_path)?;
            let run = run_histogram_reducer(config, &stage.id, &checkpoint_run, &histogram_run)?;
            write_reducer_run(&artifact_path, &run)?;
            PipelineStageResult {
                stage_id: stage.id.clone(),
                kind: stage.kind.clone(),
                artifact_path: artifact_path.display().to_string(),
                records: run.histogram.len(),
            }
        }
        other => {
            return Err(PipelineError::Config(format!(
                "unsupported stage type {}",
                other
            )))
        }
    };

    Ok(result)
}

fn median(values: &[u128]) -> u128 {
    values[values.len() / 2]
}

fn window_specs() -> [(&'static str, u64); 6] {
    [
        ("5m", 5_u64 * 60),
        ("30m", 30_u64 * 60),
        ("1h", 60_u64 * 60),
        ("6h", 6_u64 * 60 * 60),
        ("12h", 12_u64 * 60 * 60),
        ("24h", 24_u64 * 60 * 60),
    ]
}

fn map_histogram_contributions(
    checkpoints: &[ReducedCheckpoint],
    checkpoint_interval_seconds: u64,
    window_specs: &[(&str, u64)],
) -> Vec<HistogramMapOutput> {
    let mut contributions = Vec::new();

    for (checkpoint_index, checkpoint) in checkpoints.iter().enumerate() {
        for (label, seconds) in window_specs {
            let checkpoint_count =
                ((seconds + checkpoint_interval_seconds - 1) / checkpoint_interval_seconds) as usize;
            let last_anchor_index =
                (checkpoint_index + checkpoint_count).min(checkpoints.len()).saturating_sub(1);
            for anchor_index in checkpoint_index..=last_anchor_index {
                contributions.push(HistogramMapOutput {
                    anchor_index,
                    checkpoint_label: checkpoints[anchor_index].checkpoint_label.clone(),
                    block_number: checkpoints[anchor_index].block_number,
                    target_timestamp: checkpoints[anchor_index].target_timestamp,
                    window_label: (*label).to_string(),
                    checkpoint_count,
                    checkpoint_value: checkpoint.reduced_value,
                });
            }
        }
    }

    contributions
}

fn reduce_histogram_contributions(
    checkpoints: &[ReducedCheckpoint],
    window_specs: &[(&str, u64)],
    contributions: &[HistogramMapOutput],
) -> Result<Vec<ReducedAnchor>, PipelineError> {
    let mut grouped: HashMap<(usize, String), Vec<u128>> = HashMap::new();
    for contribution in contributions {
        grouped
            .entry((contribution.anchor_index, contribution.window_label.clone()))
            .or_default()
            .push(contribution.checkpoint_value);
    }

    let mut anchors = Vec::with_capacity(checkpoints.len());
    for (anchor_index, anchor) in checkpoints.iter().enumerate() {
        let mut windows = Vec::new();
        for (label, _seconds) in window_specs {
            let Some(values) = grouped.get(&(anchor_index, (*label).to_string())) else {
                continue;
            };
            let checkpoint_count = contributions
                .iter()
                .find(|contribution| {
                    contribution.anchor_index == anchor_index && contribution.window_label == *label
                })
                .map(|contribution| contribution.checkpoint_count)
                .ok_or_else(|| {
                    PipelineError::Source(format!(
                        "missing checkpoint count for anchor {} window {}",
                        anchor_index, label
                    ))
                })?;

            if values.len() < checkpoint_count {
                continue;
            }

            let sum: u128 = values.iter().sum();
            windows.push(ReducedWindow {
                label: (*label).to_string(),
                checkpoint_count,
                value: sum / checkpoint_count as u128,
            });
        }
        anchors.push(ReducedAnchor {
            checkpoint_label: anchor.checkpoint_label.clone(),
            block_number: anchor.block_number,
            target_timestamp: anchor.target_timestamp,
            windows,
        });
    }

    Ok(anchors)
}

pub fn build_checkpoints(
    config: &PipelineConfig,
    anchor_timestamp: u64,
    latest_block_number: Option<u64>,
) -> Vec<Checkpoint> {
    let mut checkpoints = Vec::with_capacity(config.checkpoints.count as usize);
    for checkpoint_index in 0..config.checkpoints.count {
        let reverse_index = config.checkpoints.count - 1 - checkpoint_index;
        let timestamp_offset_seconds = checkpoint_index as u64 * config.checkpoints.interval_seconds;
        checkpoints.push(Checkpoint {
            index: checkpoint_index,
            label: format!("t-{reverse_index}"),
            timestamp_offset_seconds,
            target_timestamp: anchor_timestamp.saturating_sub(timestamp_offset_seconds),
            block_number: latest_block_number.filter(|_| checkpoint_index == 0),
        });
    }
    checkpoints
}

pub fn current_unix_timestamp() -> Result<u64, PipelineError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| PipelineError::Source(format!("system clock before unix epoch: {err}")))?
        .as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn pipeline_fixture() -> &'static str {
        r#"
name = "zkc-price-feed"
version = 1
description = "test"

[network]
chain = "base"
settlement_chain = "base"

[asset]
base = "ZKC"
quote = "USD"

[trigger]
mode = "any"

[[trigger.conditions]]
type = "staleness"
max_age_seconds = 300

[sources.uniswap_v4_pool]
type = "uniswap_v4_pool_state"
adapter = "uniswap-v4-pool-state"
chain = "ethereum"
pool_id = "0xpool"
base_token = "ZKC"
quote_token = "USDC"
base_token_address = "0x0000000000000000000000000000000000000001"
quote_token_address = "0x0000000000000000000000000000000000000002"
base_decimals = 18
quote_decimals = 6
quote_notionals = ["100e6", "1000e6"]
normalization = "usd_via_quote"

[checkpoints]
schedule = "fixed_interval"
interval_seconds = 300
count = 3
anchor = "latest"

[mapper]
id = "historical-pool-checkpoint"
type = "historical_checkpoint"
source = "uniswap_v4_pool"
proof_boundary = "per_checkpoint_notional"

[reducer]
id = "zkc-usd-twap"
type = "twap"
input = "mapper"
window = "full_checkpoint_set"
flash_protection = "enabled"
outlier_policy = "clip_deviation"
max_point_deviation_bps = 200

[compute]
execution = "offchain"
backend = "boundless"
guest = "guest"
merklize = true

[materialization]
batch_commitment = "merkle_root"
publish = ["reduced_output"]
data_availability = "offchain"

[retention]
onchain_recent_reduced_outputs = 32
offchain_checkpoint_history = 4096

[destination]
type = "chainlink_compatible_feed"
adapter = "chainlink-feed"
output_key = "zkc-usd"
decimals = 8
publish_mode = "latest_round_only"

[requestor]
type = "boundless_smart_contract_requestor"
authorization = "trigger_gated"
callback = "chainlink-feed"

[adapters.source.uniswap-v4-pool-state]
kind = "source"
reads = ["slot0"]

[adapters.destination.chainlink-feed]
kind = "destination"
contract = "Adapter"
interface = "AggregatorV3Interface"
stores = ["latest_round"]

[[stages]]
id = "map-checkpoints"
type = "map_checkpoints"
source = "uniswap_v4_pool"
proof_boundary = "per_checkpoint_notional"

[[stages]]
id = "reduce-checkpoint-price"
type = "reduce_checkpoint_price"
input = "map-checkpoints"

[[stages]]
id = "map-histogram"
type = "map_histogram"
input = "reduce-checkpoint-price"

[[stages]]
id = "reduce-histogram"
type = "reduce_histogram"
input = "map-histogram"
"#
    }

    #[test]
    fn loads_pipeline_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline.toml");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(pipeline_fixture().as_bytes()).unwrap();

        let config = load_pipeline(&path).unwrap();
        assert_eq!(config.name, "zkc-price-feed");
        assert_eq!(config.stages.len(), 4);
        assert_eq!(
            config.sources["uniswap_v4_pool"].quote_notionals,
            vec!["100e6".to_string(), "1000e6".to_string()]
        );
    }

    #[test]
    fn expands_jobs_across_checkpoints_and_notionals() {
        let config = toml::from_str::<PipelineConfig>(pipeline_fixture()).unwrap();
        let checkpoints = build_checkpoints(&config, 1_700_000_000, Some(20_000_000));
        let jobs = expand_mapper_jobs(&config, &checkpoints).unwrap();

        assert_eq!(jobs.len(), 6);
        assert_eq!(jobs[0].map_key, "zkc-price-feed:uniswap_v4_pool:t-2:100e6");
        assert_eq!(
            jobs[5].job_id,
            "historical-pool-checkpoint:zkc-price-feed:uniswap_v4_pool:t-0:1000e6"
        );
        assert_eq!(jobs[0].checkpoint.block_number, Some(20_000_000));
    }

    #[test]
    fn dry_run_mapper_executes_without_proofs() {
        let config = toml::from_str::<PipelineConfig>(pipeline_fixture()).unwrap();
        let fixtures = HashMap::from([
            ("uniswap_v4_pool:t-2:100e6".to_string(), 101_u128),
            ("uniswap_v4_pool:t-2:1000e6".to_string(), 102),
            ("uniswap_v4_pool:t-1:100e6".to_string(), 103),
            ("uniswap_v4_pool:t-1:1000e6".to_string(), 104),
            ("uniswap_v4_pool:t-0:100e6".to_string(), 105),
            ("uniswap_v4_pool:t-0:1000e6".to_string(), 106),
        ]);
        let source = FixturePriceSource::new(fixtures);
        let resolver = StaticCheckpointResolver {
            anchor_timestamp: 1_700_000_000,
            latest_block_number: Some(20_000_000),
        };

        let run = run_mapper(
            &config,
            ExecutionMode::DryRun,
            MapperOptions::default(),
            &resolver,
            &source,
        )
        .unwrap();
        assert_eq!(run.outputs.len(), 6);
        assert!(run.outputs.iter().all(|output| output.proof_id.is_none()));
        assert_eq!(run.outputs[0].observed_value, 101);
    }

    #[test]
    fn cached_price_source_reuses_persisted_values() {
        struct CountingSource {
            calls: RefCell<u32>,
        }

        impl PriceSource for CountingSource {
            fn fetch_price(&self, _job: &MapperJob) -> Result<u128, PipelineError> {
                *self.calls.borrow_mut() += 1;
                Ok(777)
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let job = MapperJob {
            map_key: "pipeline:source:t-0:100e6".to_string(),
            job_id: "job-1".to_string(),
            source_id: "source".to_string(),
            pool_id: "pool".to_string(),
            chain: "ethereum".to_string(),
            quote_notional: "100e6".to_string(),
            checkpoint: Checkpoint {
                index: 0,
                label: "t-0".to_string(),
                timestamp_offset_seconds: 0,
                target_timestamp: 1_700_000_000,
                block_number: Some(123),
            },
        };

        let source = CountingSource {
            calls: RefCell::new(0),
        };
        let cached = CachedPriceSource::new(source, "test-quotes");
        assert_eq!(cached.fetch_price(&job).unwrap(), 777);
        assert_eq!(cached.fetch_price(&job).unwrap(), 777);

        let cache_file = dir.path().join(".delorium-cache/test-quotes.json");
        assert!(cache_file.exists());
        std::env::set_current_dir(original_dir).unwrap();
    }

    #[test]
    fn cached_price_source_key_is_stable_across_checkpoint_labels() {
        let checkpoint = Checkpoint {
            index: 0,
            label: "t-0".to_string(),
            timestamp_offset_seconds: 0,
            target_timestamp: 1_700_000_000,
            block_number: Some(123),
        };
        let job_a = MapperJob {
            map_key: "pipeline:source:t-0:100e6".to_string(),
            job_id: "job-a".to_string(),
            source_id: "source".to_string(),
            pool_id: "pool".to_string(),
            chain: "ethereum".to_string(),
            quote_notional: "100e6".to_string(),
            checkpoint: checkpoint.clone(),
        };
        let job_b = MapperJob {
            map_key: "pipeline:source:t-50:100e6".to_string(),
            job_id: "job-b".to_string(),
            source_id: "source".to_string(),
            pool_id: "pool".to_string(),
            chain: "ethereum".to_string(),
            quote_notional: "100e6".to_string(),
            checkpoint: Checkpoint {
                label: "t-50".to_string(),
                ..checkpoint
            },
        };

        let key_a = CachedPriceSource::<NoopPriceSource>::cache_key(&job_a).unwrap();
        let key_b = CachedPriceSource::<NoopPriceSource>::cache_key(&job_b).unwrap();
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn reducer_computes_per_anchor_histogram() {
        let config = toml::from_str::<PipelineConfig>(pipeline_fixture()).unwrap();
        let mapper_run = MapperRun {
            pipeline_name: "zkc-price-feed".to_string(),
            stage_id: "map-checkpoints".to_string(),
            mode: ExecutionMode::DryRun,
            outputs: vec![
                mapper_output("t-5", 1_700_000_000 - 1_500, Some(100), "100e6", 90),
                mapper_output("t-5", 1_700_000_000 - 1_500, Some(100), "1000e6", 100),
                mapper_output("t-4", 1_700_000_000 - 1_200, Some(101), "100e6", 100),
                mapper_output("t-4", 1_700_000_000 - 1_200, Some(101), "1000e6", 110),
                mapper_output("t-3", 1_700_000_000 - 900, Some(102), "100e6", 110),
                mapper_output("t-3", 1_700_000_000 - 900, Some(102), "1000e6", 120),
                mapper_output("t-2", 1_700_000_000 - 600, Some(103), "100e6", 120),
                mapper_output("t-2", 1_700_000_000 - 600, Some(103), "1000e6", 130),
                mapper_output("t-1", 1_700_000_000 - 300, Some(104), "100e6", 130),
                mapper_output("t-1", 1_700_000_000 - 300, Some(104), "1000e6", 140),
                mapper_output("t-0", 1_700_000_000, Some(105), "100e6", 140),
                mapper_output("t-0", 1_700_000_000, Some(105), "1000e6", 150),
            ],
        };

        let reduced = run_reducer(&config, &mapper_run).unwrap();
        assert_eq!(reduced.checkpoints.len(), 6);
        assert_eq!(reduced.checkpoints[0].reduced_value, 100);
        assert_eq!(reduced.checkpoints[1].reduced_value, 110);
        assert_eq!(reduced.checkpoints[5].reduced_value, 150);
        assert_eq!(reduced.histogram.len(), 6);
        assert_eq!(
            reduced.histogram[0].windows,
            vec![ReducedWindow {
                label: "5m".to_string(),
                checkpoint_count: 1,
                value: 100,
            }]
        );
        assert_eq!(
            reduced.histogram[5].windows,
            vec![
                ReducedWindow {
                    label: "5m".to_string(),
                    checkpoint_count: 1,
                    value: 150,
                },
                ReducedWindow {
                    label: "30m".to_string(),
                    checkpoint_count: 6,
                    value: 125,
                },
            ]
        );
    }

    #[test]
    fn staged_pipeline_runner_writes_all_artifacts() {
        let config = toml::from_str::<PipelineConfig>(pipeline_fixture()).unwrap();
        let fixtures = HashMap::from([
            ("uniswap_v4_pool:t-2:100e6".to_string(), 101_u128),
            ("uniswap_v4_pool:t-2:1000e6".to_string(), 102),
            ("uniswap_v4_pool:t-1:100e6".to_string(), 103),
            ("uniswap_v4_pool:t-1:1000e6".to_string(), 104),
            ("uniswap_v4_pool:t-0:100e6".to_string(), 105),
            ("uniswap_v4_pool:t-0:1000e6".to_string(), 106),
        ]);
        let source = FixturePriceSource::new(fixtures);
        let resolver = StaticCheckpointResolver {
            anchor_timestamp: 1_700_000_000,
            latest_block_number: Some(20_000_000),
        };
        let dir = tempfile::tempdir().unwrap();

        let run = run_pipeline(
            &config,
            ExecutionMode::DryRun,
            MapperOptions::default(),
            &resolver,
            &source,
            dir.path(),
        )
        .unwrap();

        assert_eq!(run.stages.len(), 4);
        assert!(dir.path().join("01-map-checkpoints.mapper.json").exists());
        assert!(dir.path().join("02-reduce-checkpoint-price.checkpoint.json").exists());
        assert!(dir.path().join("03-map-histogram.histogram-map.json").exists());
        assert!(dir.path().join("04-reduce-histogram.histogram.json").exists());
    }

    #[test]
    fn renders_histogram_chart_html() {
        let run = ReducerRun {
            pipeline_name: "zkc-price-feed".to_string(),
            reducer_id: "reduce-histogram".to_string(),
            reducer_kind: "twap".to_string(),
            checkpoints: vec![],
            histogram: vec![
                ReducedAnchor {
                    checkpoint_label: "t-1".to_string(),
                    block_number: Some(100),
                    target_timestamp: 1_700_000_000,
                    windows: vec![ReducedWindow {
                        label: "5m".to_string(),
                        checkpoint_count: 1,
                        value: 100,
                    }],
                },
                ReducedAnchor {
                    checkpoint_label: "t-0".to_string(),
                    block_number: Some(101),
                    target_timestamp: 1_700_000_300,
                    windows: vec![
                        ReducedWindow {
                            label: "5m".to_string(),
                            checkpoint_count: 1,
                            value: 110,
                        },
                        ReducedWindow {
                            label: "30m".to_string(),
                            checkpoint_count: 6,
                            value: 105,
                        },
                    ],
                },
            ],
        };

        let html = render_histogram_chart(&run, 8).unwrap();
        assert!(html.contains("zkc-price-feed Histogram"));
        assert!(html.contains("5m"));
        assert!(html.contains("30m"));
        assert!(html.contains("<svg"));
        assert!(html.contains("<polyline"));
        assert!(html.contains("$0.00000100"));
        assert!(html.contains("11-14 22:13"));
    }

    fn mapper_output(
        label: &str,
        timestamp: u64,
        block_number: Option<u64>,
        notional: &str,
        value: u128,
    ) -> MapperOutput {
        MapperOutput {
            job: MapperJob {
                map_key: format!("zkc-price-feed:uniswap_v4_pool:{label}:{notional}"),
                job_id: format!("job:{label}:{notional}"),
                source_id: "uniswap_v4_pool".to_string(),
                pool_id: "0xpool".to_string(),
                chain: "ethereum".to_string(),
                quote_notional: notional.to_string(),
                checkpoint: Checkpoint {
                    index: 0,
                    label: label.to_string(),
                    timestamp_offset_seconds: 0,
                    target_timestamp: timestamp,
                    block_number,
                },
            },
            observed_value: value,
            proof_id: None,
        }
    }
}
