use std::{collections::BTreeMap, fs, path::Path};

use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::report::CanonicalRunReport;

#[derive(Debug, Clone)]
pub struct CompareConfig {
    pub throughput_regression_pct: f64,
    pub p99_regression_pct: f64,
}

#[derive(Debug, Clone)]
pub struct CompareOutcome {
    pub within_noise_budget: bool,
    pub throughput_delta_pct: f64,
    pub p99_delta_pct: Option<f64>,
}

pub fn compare_reports(
    baseline_path: &Path,
    candidate_path: &Path,
    config: &CompareConfig,
) -> Result<CompareOutcome> {
    let baseline = read_report(baseline_path)?;
    let candidate = read_report(candidate_path)?;

    ensure_metadata_match(&baseline, &candidate)?;

    let throughput_floor = baseline.ops_per_sec * (1.0 - config.throughput_regression_pct / 100.0);
    let throughput_delta_pct = pct_delta(candidate.ops_per_sec, baseline.ops_per_sec);
    if candidate.ops_per_sec < throughput_floor {
        bail!(
            "throughput regression beyond tolerance: candidate {:.3} < floor {:.3}",
            candidate.ops_per_sec,
            throughput_floor
        );
    }

    let p99_delta_pct = match (&baseline.latency, &candidate.latency) {
        (Some(baseline_latency), Some(candidate_latency)) => {
            let ceiling = baseline_latency.p99_ms * (1.0 + config.p99_regression_pct / 100.0);
            let delta = pct_delta(candidate_latency.p99_ms, baseline_latency.p99_ms);
            if candidate_latency.p99_ms > ceiling {
                bail!(
                    "p99 regression beyond tolerance: candidate {:.3} > ceiling {:.3}",
                    candidate_latency.p99_ms,
                    ceiling
                );
            }
            Some(delta)
        }
        (None, None) => None,
        _ => bail!("metadata mismatch: latency presence differs"),
    };

    Ok(CompareOutcome {
        within_noise_budget: true,
        throughput_delta_pct,
        p99_delta_pct,
    })
}

fn read_report(path: &Path) -> Result<CanonicalRunReport> {
    let payload = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&payload).with_context(|| format!("parse {}", path.display()))
}

fn ensure_metadata_match(baseline: &CanonicalRunReport, candidate: &CanonicalRunReport) -> Result<()> {
    if baseline.mode != candidate.mode {
        bail!(
            "metadata mismatch: mode {} != {}",
            baseline.mode,
            candidate.mode
        );
    }
    if baseline.public_report != candidate.public_report {
        bail!("metadata mismatch: public_report differs");
    }
    if baseline.comparison_arms != candidate.comparison_arms {
        bail!("metadata mismatch: comparison_arms differ");
    }

    let baseline_options = comparable_options(&baseline.options);
    let candidate_options = comparable_options(&candidate.options);
    if baseline_options != candidate_options {
        bail!("metadata mismatch: comparable options differ");
    }

    let baseline_metadata = comparable_metadata(&baseline.metadata);
    let candidate_metadata = comparable_metadata(&candidate.metadata);
    if baseline_metadata != candidate_metadata {
        bail!("metadata mismatch: metadata fields differ");
    }

    Ok(())
}

fn comparable_options(options: &BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    options
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "report_file" | "artifacts_root" | "storage_path"
            )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn comparable_metadata(metadata: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    metadata
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "storage_path" | "addr" | "url"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn pct_delta(candidate: f64, baseline: f64) -> f64 {
    if baseline.abs() < f64::EPSILON {
        return 0.0;
    }
    ((candidate - baseline) / baseline) * 100.0
}
