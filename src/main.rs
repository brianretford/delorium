use std::env;
use std::fs;
use std::process::ExitCode;

use delorium::{
    current_unix_timestamp, expand_mapper_jobs, load_pipeline, run_mapper, summarize_mapper_run,
    AlchemyCheckpointResolver, AlchemySpotPriceSource, CachedPriceSource, CheckpointResolver,
    ExecutionMode, FixturePriceSource, MapperOptions, NoopPriceSource, StaticCheckpointResolver,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        return Err(
            "usage: delorium map <pipeline.toml> [dry-run|prove] [local|alchemy] [checkpoint_limit]"
                .into(),
        );
    }

    if args[1] != "map" {
        return Err(format!("unknown command {}", args[1]).into());
    }

    let mode = if args.len() >= 4 {
        ExecutionMode::from_str(&args[3])?
    } else {
        ExecutionMode::DryRun
    };
    let resolver_mode = if args.len() >= 5 { &args[4] } else { "local" };
    let checkpoint_limit = if args.len() >= 6 {
        Some(args[5].parse::<u32>()?)
    } else {
        None
    };
    let options = MapperOptions { checkpoint_limit };

    let config = load_pipeline(&args[2])?;
    let source_config = config.sources[&config.mapper.source].clone();
    let alchemy_key = if resolver_mode == "alchemy" {
        Some(load_alchemy_key()?)
    } else {
        None
    };
    let resolver_box: Box<dyn CheckpointResolver> = match resolver_mode {
        "local" => Box::new(StaticCheckpointResolver {
            anchor_timestamp: current_unix_timestamp()?,
            latest_block_number: None,
        }),
        "alchemy" => {
            Box::new(AlchemyCheckpointResolver::from_endpoint_or_key(
                alchemy_key
                    .as_deref()
                    .ok_or("missing alchemy key")?
                    .trim(),
                &config.sources[&config.mapper.source].chain,
            )?)
        }
        other => {
            return Err(format!("unknown resolver mode {other}").into());
        }
    };

    let mut checkpoints = resolver_box.resolve(&config)?;
    if let Some(limit) = checkpoint_limit {
        checkpoints.truncate(limit as usize);
    }
    let jobs = expand_mapper_jobs(&config, &checkpoints)?;
    for job in &jobs {
        println!(
            "job={} map_key={} chain={} block={} timestamp={} notional={} pool={}",
            job.job_id,
            job.map_key,
            job.chain,
            job.checkpoint
                .block_number
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unresolved".to_string()),
            job.checkpoint.target_timestamp,
            job.quote_notional,
            job.pool_id,
        );
    }

    let run = match mode {
        ExecutionMode::DryRun => {
            let source: Box<dyn delorium::PriceSource> = match resolver_mode {
                "alchemy" => Box::new(CachedPriceSource::new(
                    AlchemySpotPriceSource::from_config(
                        &source_config,
                        alchemy_key.as_deref().ok_or("missing alchemy key")?,
                    )?,
                    "mapper-quotes",
                )),
                _ => Box::new(CachedPriceSource::new(NoopPriceSource, "mapper-quotes")),
            };
            run_mapper(&config, mode, options, resolver_box.as_ref(), source.as_ref())?
        }
        ExecutionMode::Prove => {
            let fixtures = std::collections::HashMap::new();
            let source = CachedPriceSource::new(FixturePriceSource::new(fixtures), "mapper-quotes");
            run_mapper(&config, mode, options, resolver_box.as_ref(), &source)?
        }
    };
    for output in &run.outputs {
        println!(
            "observed map_key={} block={} value={}",
            output.job.map_key,
            output
                .job
                .checkpoint
                .block_number
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unresolved".to_string()),
            output.observed_value
        );
    }
    println!("{}", summarize_mapper_run(&run));

    Ok(())
}

fn load_alchemy_key() -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(value) = env::var("ALCHEMY_KEY") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    for path in ["ALCHEMY_KEY", "RPCKEY"] {
        if let Ok(value) = fs::read_to_string(path) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
    }

    Err("missing ALCHEMY_KEY env var or ALCHEMY_KEY file".into())
}
