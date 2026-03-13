use std::env;
use std::fs;
use std::process::ExitCode;

use delorium::{
    current_unix_timestamp, load_mapper_run, load_pipeline, load_reducer_run, render_histogram_chart,
    run_checkpoint_reducer, run_histogram_mapper, run_histogram_reducer, run_mapper,
    run_mapper_stage_with_progress, run_pipeline_stage, run_reducer, stage_artifact_path,
    summarize_mapper_run, summarize_pipeline_run, summarize_reducer_run, write_chart_html,
    write_checkpoint_run, write_histogram_map_run, write_mapper_run, write_reducer_run,
    AlchemyCheckpointResolver, AlchemySpotPriceSource, CachedPriceSource, CheckpointResolver,
    ExecutionMode, FixturePriceSource, MapperOptions, NoopPriceSource, PipelineRunSummary,
    PipelineStageResult, StaticCheckpointResolver, pipeline_stages,
};
use indicatif::{ProgressBar, ProgressStyle};

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
        return Err("usage: delorium <run|run-stage|map|reduce|render> ...".into());
    }

    match args[1].as_str() {
        "run" => run_pipeline_cmd(&args),
        "run-stage" => run_stage_cmd(&args),
        "map" => run_map_compat(&args),
        "reduce" => run_reduce_compat(&args),
        "render" => run_render_cmd(&args),
        other => Err(format!("unknown command {other}").into()),
    }
}

fn run_pipeline_cmd(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() < 3 {
        return Err("usage: delorium run <pipeline.toml> [dry-run|prove] [local|alchemy] [checkpoint_limit] [output_dir]".into());
    }
    let mode = if args.len() >= 4 {
        ExecutionMode::from_str(&args[3])?
    } else {
        ExecutionMode::DryRun
    };
    let resolver_mode = if args.len() >= 5 { &args[4] } else { "local" };
    let (checkpoint_limit, output_dir) =
        parse_optional_limit_and_path(args.get(5), args.get(6), ".delorium-runs/latest")?;

    let config = load_pipeline(&args[2])?;
    let options = MapperOptions { checkpoint_limit };
    let (resolver_box, source_box) = build_runtime(&config, resolver_mode, mode)?;
    let run = run_pipeline_with_progress(
        &config,
        mode,
        options,
        resolver_box.as_ref(),
        source_box.as_ref(),
        output_dir,
    )?;

    for stage in &run.stages {
        println!(
            "stage={} kind={} records={} artifact={}",
            stage.stage_id, stage.kind, stage.records, stage.artifact_path
        );
    }
    println!("{}", summarize_pipeline_run(&run));
    Ok(())
}

fn run_stage_cmd(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() < 4 {
        return Err("usage: delorium run-stage <pipeline.toml> <stage-id> [dry-run|prove] [local|alchemy] [checkpoint_limit] [output_dir]".into());
    }
    let mode = if args.len() >= 5 {
        ExecutionMode::from_str(&args[4])?
    } else {
        ExecutionMode::DryRun
    };
    let resolver_mode = if args.len() >= 6 { &args[5] } else { "local" };
    let (checkpoint_limit, output_dir) =
        parse_optional_limit_and_path(args.get(6), args.get(7), ".delorium-runs/latest")?;

    let config = load_pipeline(&args[2])?;
    let options = MapperOptions { checkpoint_limit };
    let (resolver_box, source_box) = build_runtime(&config, resolver_mode, mode)?;
    let result = run_pipeline_stage_with_progress(
        &config,
        &args[3],
        mode,
        options,
        resolver_box.as_ref(),
        source_box.as_ref(),
        output_dir,
    )?;
    println!(
        "stage={} kind={} records={} artifact={}",
        result.stage_id, result.kind, result.records, result.artifact_path
    );
    Ok(())
}

fn run_pipeline_with_progress(
    config: &delorium::PipelineConfig,
    mode: ExecutionMode,
    options: MapperOptions,
    checkpoint_resolver: &dyn CheckpointResolver,
    source: &dyn delorium::PriceSource,
    output_dir: &str,
) -> Result<PipelineRunSummary, Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;
    let stages = pipeline_stages(config);
    let total_stages = stages.len();
    let mut results = Vec::with_capacity(total_stages);
    let mut mapper_run = None;
    let mut checkpoint_run = None;
    let mut histogram_map_run = None;

    for (index, stage) in stages.iter().enumerate() {
        print_stage_header(index + 1, total_stages, &stage.id, &stage.kind)?;
        let artifact_path = stage_artifact_path(output_dir, index, stage);
        let result = match stage.kind.as_str() {
            "map_checkpoints" => {
                let progress = stage_progress_bar(&stage.id);
                start_checkpoint_phase(
                    &progress,
                    expected_checkpoint_count(config, options.checkpoint_limit),
                );
                let run = run_mapper_stage_with_progress(
                    config,
                    &stage.id,
                    mode,
                    options,
                    checkpoint_resolver,
                    source,
                    &mut |completed, total| {
                        update_stage_progress(
                            &progress,
                            completed,
                            total,
                            "resolving checkpoints".to_string(),
                        );
                    },
                    |completed, total, job| {
                        if completed == 1 {
                            start_mapper_phase(
                                &progress,
                                expected_mapper_jobs(config, options.checkpoint_limit),
                            );
                        }
                        update_stage_progress(
                            &progress,
                            completed,
                            total,
                            format!(
                                "fetching checkpoint={} notional={}",
                                job.checkpoint.label, job.quote_notional
                            ),
                        );
                    },
                )?;
                if run.outputs.is_empty()
                    && expected_mapper_jobs(config, options.checkpoint_limit) == 0
                {
                    finish_stage_progress(&progress, "completed records=0".to_string());
                } else {
                    finish_stage_progress(
                        &progress,
                        format!("completed records={}", run.outputs.len()),
                    );
                }
                write_mapper_run(&artifact_path, &run)?;
                mapper_run = Some(run);
                PipelineStageResult {
                    stage_id: stage.id.clone(),
                    kind: stage.kind.clone(),
                    artifact_path: artifact_path.display().to_string(),
                    records: mapper_run.as_ref().map(|run| run.outputs.len()).unwrap_or(0),
                }
            }
            "reduce_checkpoint_price" => {
                let input = mapper_run
                    .as_ref()
                    .ok_or("missing mapper input for reduce_checkpoint_price")?;
                let run = run_checkpoint_reducer(config, &stage.id, input)?;
                write_checkpoint_run(&artifact_path, &run)?;
                checkpoint_run = Some(run);
                PipelineStageResult {
                    stage_id: stage.id.clone(),
                    kind: stage.kind.clone(),
                    artifact_path: artifact_path.display().to_string(),
                    records: checkpoint_run
                        .as_ref()
                        .map(|run| run.checkpoints.len())
                        .unwrap_or(0),
                }
            }
            "map_histogram" => {
                let input = checkpoint_run
                    .as_ref()
                    .ok_or("missing checkpoint input for map_histogram")?;
                let run = run_histogram_mapper(config, &stage.id, input)?;
                write_histogram_map_run(&artifact_path, &run)?;
                histogram_map_run = Some(run);
                PipelineStageResult {
                    stage_id: stage.id.clone(),
                    kind: stage.kind.clone(),
                    artifact_path: artifact_path.display().to_string(),
                    records: histogram_map_run
                        .as_ref()
                        .map(|run| run.outputs.len())
                        .unwrap_or(0),
                }
            }
            "reduce_histogram" => {
                let checkpoint_input = checkpoint_run
                    .as_ref()
                    .ok_or("missing checkpoint input for reduce_histogram")?;
                let histogram_input = histogram_map_run
                    .as_ref()
                    .ok_or("missing histogram input for reduce_histogram")?;
                let run =
                    run_histogram_reducer(config, &stage.id, checkpoint_input, histogram_input)?;
                write_reducer_run(&artifact_path, &run)?;
                PipelineStageResult {
                    stage_id: stage.id.clone(),
                    kind: stage.kind.clone(),
                    artifact_path: artifact_path.display().to_string(),
                    records: run.histogram.len(),
                }
            }
            other => return Err(format!("unsupported stage type {other}").into()),
        };
        print_stage_footer(index + 1, total_stages, &result)?;
        results.push(result);
    }

    Ok(PipelineRunSummary {
        pipeline_name: config.name.clone(),
        mode,
        stages: results,
    })
}

fn run_pipeline_stage_with_progress(
    config: &delorium::PipelineConfig,
    stage_id: &str,
    mode: ExecutionMode,
    options: MapperOptions,
    checkpoint_resolver: &dyn CheckpointResolver,
    source: &dyn delorium::PriceSource,
    output_dir: &str,
) -> Result<PipelineStageResult, Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;
    let stages = pipeline_stages(config);
    let stage_index = stages
        .iter()
        .position(|stage| stage.id == stage_id)
        .ok_or_else(|| format!("unknown stage {stage_id}"))?;
    let stage = &stages[stage_index];
    let artifact_path = stage_artifact_path(output_dir, stage_index, stage);

    print_stage_header(stage_index + 1, stages.len(), &stage.id, &stage.kind)?;
    let result = match stage.kind.as_str() {
        "map_checkpoints" => {
            let progress = stage_progress_bar(&stage.id);
            start_checkpoint_phase(
                &progress,
                expected_checkpoint_count(config, options.checkpoint_limit),
            );
            let run = run_mapper_stage_with_progress(
                config,
                &stage.id,
                mode,
                options,
                checkpoint_resolver,
                source,
                &mut |completed, total| {
                    update_stage_progress(
                        &progress,
                        completed,
                        total,
                        "resolving checkpoints".to_string(),
                    );
                },
                |completed, total, job| {
                    if completed == 1 {
                        start_mapper_phase(
                            &progress,
                            expected_mapper_jobs(config, options.checkpoint_limit),
                        );
                    }
                    update_stage_progress(
                        &progress,
                        completed,
                        total,
                        format!(
                            "fetching checkpoint={} notional={}",
                            job.checkpoint.label, job.quote_notional
                        ),
                    );
                },
            )?;
            if run.outputs.is_empty()
                && expected_mapper_jobs(config, options.checkpoint_limit) == 0
            {
                finish_stage_progress(&progress, "completed records=0".to_string());
            } else {
                finish_stage_progress(
                    &progress,
                    format!("completed records={}", run.outputs.len()),
                );
            }
            write_mapper_run(&artifact_path, &run)?;
            PipelineStageResult {
                stage_id: stage.id.clone(),
                kind: stage.kind.clone(),
                artifact_path: artifact_path.display().to_string(),
                records: run.outputs.len(),
            }
        }
        _ => {
            let result = run_pipeline_stage(
                config,
                stage_id,
                mode,
                options,
                checkpoint_resolver,
                source,
                output_dir,
            )?;
            result
        }
    };
    print_stage_footer(stage_index + 1, stages.len(), &result)?;
    Ok(result)
}

fn print_stage_header(
    current: usize,
    total: usize,
    stage_id: &str,
    kind: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("[{current}/{total}] {stage_id} ({kind})");
    Ok(())
}

fn print_stage_footer(
    current: usize,
    total: usize,
    result: &PipelineStageResult,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!(
        "[{current}/{total}] done stage={} records={} artifact={}",
        result.stage_id, result.records, result.artifact_path
    );
    Ok(())
}

fn stage_progress_bar(stage_id: &str) -> ProgressBar {
    let progress = ProgressBar::new_spinner();
    progress.enable_steady_tick(std::time::Duration::from_millis(120));
    progress.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} {msg} [{bar:30.cyan/blue}] {pos}/{len} elapsed={elapsed_precise}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    progress.set_prefix(stage_id.to_string());
    progress
}

fn start_checkpoint_phase(progress: &ProgressBar, expected_total: usize) {
    progress.set_length(expected_total as u64);
    progress.set_position(0);
    progress.set_message("resolving checkpoints".to_string());
}

fn start_mapper_phase(progress: &ProgressBar, expected_total: usize) {
    progress.set_length(expected_total as u64);
    progress.set_position(0);
    progress.set_message("fetching quotes".to_string());
}

fn update_stage_progress(
    progress: &ProgressBar,
    completed: usize,
    total: usize,
    message: String,
) {
    progress.set_length(total as u64);
    progress.set_position(completed as u64);
    progress.set_message(message);
}

fn finish_stage_progress(progress: &ProgressBar, message: String) {
    progress.finish_with_message(message);
}

fn expected_checkpoint_count(
    config: &delorium::PipelineConfig,
    checkpoint_limit: Option<u32>,
) -> usize {
    checkpoint_limit
        .unwrap_or(config.checkpoints.count)
        .min(config.checkpoints.count) as usize
}

fn expected_mapper_jobs(config: &delorium::PipelineConfig, checkpoint_limit: Option<u32>) -> usize {
    let checkpoint_count = expected_checkpoint_count(config, checkpoint_limit);
    let source_id = config
        .stages
        .iter()
        .find(|stage| stage.kind == "map_checkpoints")
        .and_then(|stage| stage.source.as_ref())
        .unwrap_or(&config.mapper.source);
    let notional_count = config
        .sources
        .get(source_id)
        .map(|source| source.quote_notionals.len())
        .unwrap_or(0);
    checkpoint_count * notional_count
}

fn run_map_compat(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mode = if args.len() >= 4 {
        ExecutionMode::from_str(&args[3])?
    } else {
        ExecutionMode::DryRun
    };
    let resolver_mode = if args.len() >= 5 { &args[4] } else { "local" };
    let (checkpoint_limit, output_path) =
        parse_optional_limit_and_optional_path(args.get(5), args.get(6))?;
    let options = MapperOptions { checkpoint_limit };

    let config = load_pipeline(&args[2])?;
    let (resolver_box, source_box) = build_runtime(&config, resolver_mode, mode)?;
    let mut checkpoints = resolver_box.resolve(&config)?;
    if let Some(limit) = checkpoint_limit {
        checkpoints.truncate(limit as usize);
    }
    let jobs = delorium::expand_mapper_jobs(&config, &checkpoints)?;
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

    let run = run_mapper(
        &config,
        mode,
        options,
        resolver_box.as_ref(),
        source_box.as_ref(),
    )?;
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
    if let Some(path) = output_path {
        write_mapper_run(path, &run)?;
    }
    println!("{}", summarize_mapper_run(&run));
    Ok(())
}

fn run_reduce_compat(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() < 4 {
        return Err("usage: delorium reduce <pipeline.toml> <mapper-run.json> [output.json]".into());
    }

    let config = load_pipeline(&args[2])?;
    let mapper_run = load_mapper_run(&args[3])?;
    let reduced = run_reducer(&config, &mapper_run)?;
    for anchor in &reduced.histogram {
        for window in &anchor.windows {
            println!(
                "anchor={} block={} window={} checkpoints={} value={}",
                anchor.checkpoint_label,
                anchor
                    .block_number
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unresolved".to_string()),
                window.label,
                window.checkpoint_count,
                window.value
            );
        }
    }
    if args.len() >= 5 {
        write_reducer_run(&args[4], &reduced)?;
    }
    println!("{}", summarize_reducer_run(&reduced));
    Ok(())
}

fn run_render_cmd(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if args.len() < 3 {
        return Err("usage: delorium render <pipeline.toml> <reducer-run.json> [output.html]".into());
    }

    let (config, reducer_path, output_path) = if args.len() >= 4 && args[2].ends_with(".toml") {
        (
            Some(load_pipeline(&args[2])?),
            args[3].as_str(),
            if args.len() >= 5 {
                args[4].as_str()
            } else {
                "histogram.html"
            },
        )
    } else {
        (
            None,
            args[2].as_str(),
            if args.len() >= 4 {
                args[3].as_str()
            } else {
                "histogram.html"
            },
        )
    };

    let reducer_run = load_reducer_run(reducer_path)?;
    let display_decimals = config
        .as_ref()
        .map(|config| config.destination.decimals)
        .unwrap_or(8);
    let html = render_histogram_chart(&reducer_run, display_decimals)?;
    write_chart_html(output_path, &html)?;
    println!("rendered={}", output_path);
    Ok(())
}

fn build_runtime(
    config: &delorium::PipelineConfig,
    resolver_mode: &str,
    mode: ExecutionMode,
) -> Result<
    (
        Box<dyn CheckpointResolver>,
        Box<dyn delorium::PriceSource>,
    ),
    Box<dyn std::error::Error>,
> {
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
        "alchemy" => Box::new(AlchemyCheckpointResolver::from_endpoint_or_key(
            alchemy_key
                .as_deref()
                .ok_or("missing alchemy key")?
                .trim(),
            &source_config.chain,
        )?),
        other => {
            return Err(format!("unknown resolver mode {other}").into());
        }
    };

    let source_box: Box<dyn delorium::PriceSource> = match mode {
        ExecutionMode::DryRun => match resolver_mode {
            "alchemy" => Box::new(CachedPriceSource::new(
                AlchemySpotPriceSource::from_config(
                    &source_config,
                    alchemy_key.as_deref().ok_or("missing alchemy key")?,
                )?,
                "mapper-quotes",
            )),
            _ => Box::new(CachedPriceSource::new(NoopPriceSource, "mapper-quotes")),
        },
        ExecutionMode::Prove => {
            let fixtures = std::collections::HashMap::new();
            Box::new(CachedPriceSource::new(
                FixturePriceSource::new(fixtures),
                "mapper-quotes",
            ))
        }
    };

    Ok((resolver_box, source_box))
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

fn parse_optional_limit_and_path<'a>(
    first: Option<&'a String>,
    second: Option<&'a String>,
    default_path: &'a str,
) -> Result<(Option<u32>, &'a str), Box<dyn std::error::Error>> {
    match (first, second) {
        (Some(first), Some(second)) => Ok((Some(first.parse::<u32>()?), second.as_str())),
        (Some(first), None) => match first.parse::<u32>() {
            Ok(limit) => Ok((Some(limit), default_path)),
            Err(_) => Ok((None, first.as_str())),
        },
        (None, None) => Ok((None, default_path)),
        (None, Some(_)) => Ok((None, default_path)),
    }
}

fn parse_optional_limit_and_optional_path<'a>(
    first: Option<&'a String>,
    second: Option<&'a String>,
) -> Result<(Option<u32>, Option<&'a str>), Box<dyn std::error::Error>> {
    match (first, second) {
        (Some(first), Some(second)) => Ok((Some(first.parse::<u32>()?), Some(second.as_str()))),
        (Some(first), None) => match first.parse::<u32>() {
            Ok(limit) => Ok((Some(limit), None)),
            Err(_) => Ok((None, Some(first.as_str()))),
        },
        (None, None) => Ok((None, None)),
        (None, Some(second)) => Ok((None, Some(second.as_str()))),
    }
}
