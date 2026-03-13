use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub index: u32,
    pub label: String,
    pub timestamp_offset_seconds: u64,
    pub target_timestamp: u64,
    pub block_number: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapperJob {
    pub map_key: String,
    pub job_id: String,
    pub source_id: String,
    pub pool_id: String,
    pub chain: String,
    pub quote_notional: String,
    pub checkpoint: Checkpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapperOutput {
    pub job: MapperJob,
    pub observed_value: u128,
    pub proof_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapperRun {
    pub pipeline_name: String,
    pub mode: ExecutionMode,
    pub outputs: Vec<MapperOutput>,
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
        Ok(format!("{}@{}", job.map_key, locator))
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

    fn block_by_number(&self, number: u64) -> Result<BlockHeader, PipelineError> {
        self.rpc(
            "eth_getBlockByNumber",
            json!([format!("0x{number:x}"), false]),
        )
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

    fn block_timestamp_cached(
        &self,
        number: u64,
        cache: &mut AlchemyCache,
    ) -> Result<u64, PipelineError> {
        if let Some(timestamp) = cache.block_timestamps.get(&number) {
            return Ok(*timestamp);
        }

        let block = self.block_by_number(number)?;
        let timestamp = parse_hex_u64(&block.timestamp)?;
        cache.block_timestamps.insert(number, timestamp);
        Ok(timestamp)
    }

    fn find_block_at_or_before(
        &self,
        target_timestamp: u64,
        cache: &mut AlchemyCache,
    ) -> Result<u64, PipelineError> {
        if let Some(block_number) = cache.resolved_targets.get(&target_timestamp) {
            return Ok(*block_number);
        }

        let (latest_number, latest_timestamp) = self.latest_block_cached(cache)?;
        if target_timestamp >= latest_timestamp {
            cache.resolved_targets.insert(target_timestamp, latest_number);
            return Ok(latest_number);
        }

        let earliest_timestamp = self.block_timestamp_cached(0, cache)?;
        if target_timestamp <= earliest_timestamp {
            cache.resolved_targets.insert(target_timestamp, 0);
            return Ok(0);
        }

        let mut low = 0_u64;
        let mut high = latest_number;
        while low + 1 < high {
            let mid = low + (high - low) / 2;
            let timestamp = self.block_timestamp_cached(mid, cache)?;
            if timestamp <= target_timestamp {
                low = mid;
            } else {
                high = mid;
            }
        }

        cache.resolved_targets.insert(target_timestamp, low);
        Ok(low)
    }
}

fn alchemy_rpc_url(endpoint_or_key: &str, chain: &str) -> Result<String, PipelineError> {
    if endpoint_or_key.starts_with("http://") || endpoint_or_key.starts_with("https://") {
        return Ok(endpoint_or_key.to_string());
    }

    let network = match chain {
        "ethereum" => "eth-mainnet",
        "base" => "base-mainnet",
        other => {
            return Err(PipelineError::Config(format!(
                "unsupported alchemy chain {other}"
            )))
        }
    };
    Ok(format!("https://{network}.g.alchemy.com/v2/{endpoint_or_key}"))
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
        let mut cache = self.load_cache();
        let (latest_block_number, anchor_timestamp) = self.latest_block_cached(&mut cache)?;
        let mut checkpoints = build_checkpoints(config, anchor_timestamp, Some(latest_block_number));
        for checkpoint in &mut checkpoints {
            checkpoint.block_number =
                Some(self.find_block_at_or_before(checkpoint.target_timestamp, &mut cache)?);
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
    let source = config
        .sources
        .get(&config.mapper.source)
        .ok_or_else(|| PipelineError::Config(format!("unknown mapper source {}", config.mapper.source)))?;
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
    Ok(())
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
    let mut checkpoints = checkpoint_resolver.resolve(config)?;
    if let Some(limit) = options.checkpoint_limit {
        checkpoints.truncate(limit as usize);
    }
    let jobs = expand_mapper_jobs(config, &checkpoints)?;
    let outputs = jobs
        .into_iter()
        .map(|job| {
            let observed_value = source.fetch_price(&job)?;
            let proof_id = match mode {
                ExecutionMode::DryRun => None,
                ExecutionMode::Prove => Some(format!("proof:{}", job.job_id)),
            };
            Ok(MapperOutput {
                job,
                observed_value,
                proof_id,
            })
        })
        .collect::<Result<Vec<_>, PipelineError>>()?;

    Ok(MapperRun {
        pipeline_name: config.name.clone(),
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
}
