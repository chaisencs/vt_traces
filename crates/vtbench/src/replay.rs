use std::{
    collections::BTreeMap,
    env,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use serde::Serialize;
use crate::report::{
    CanonicalRunManifest, CanonicalRunReport, CommandTrace, ContextSnapshot, DecisionSnapshot,
    ManifestFiles,
};

#[derive(Debug, Clone)]
pub struct GitMetadata {
    pub sha: String,
    pub dirty_tree: bool,
}

#[derive(Debug, Clone)]
pub struct ReplayMetadata {
    pub replay_id: String,
    pub cwd: PathBuf,
    pub git: GitMetadata,
    pub executable_path: PathBuf,
    pub raw_args: Vec<String>,
    pub command_display: String,
}

#[derive(Debug, Clone)]
pub struct ArtifactBundle {
    pub run_dir: PathBuf,
}

#[derive(Debug, Serialize)]
struct EnvironmentManifest {
    cwd: String,
    os: String,
    arch: String,
    cpu_count: usize,
    git_sha: String,
    dirty_tree: bool,
    env: BTreeMap<String, String>,
}

impl ReplayMetadata {
    pub fn capture(mode: &str, raw_args: &[String]) -> Result<Self> {
        let cwd = env::current_dir().context("resolve current directory")?;
        let git = capture_git_metadata(&cwd)?;
        let replay_id = build_replay_id(mode);
        let executable_path = env::current_exe().context("resolve current executable")?;
        let command_display = build_command_display(&executable_path, mode, raw_args);
        Ok(Self {
            replay_id,
            cwd,
            git,
            executable_path,
            raw_args: raw_args.to_vec(),
            command_display,
        })
    }
}

impl ArtifactBundle {
    pub fn prepare(root: &Path, replay_id: &str) -> Result<Self> {
        let run_dir = root.join(replay_id);
        fs::create_dir_all(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;
        Ok(Self { run_dir })
    }

    pub fn write_all(
        &self,
        metadata: &ReplayMetadata,
        manifest: &CanonicalRunManifest,
        report: &CanonicalRunReport,
        context: &ContextSnapshot,
        decision: &DecisionSnapshot,
        metrics_text: &str,
    ) -> Result<()> {
        write_json(&self.run_dir.join("manifest.json"), manifest)?;
        write_json(&self.run_dir.join("report.json"), report)?;
        write_json(&self.run_dir.join("context.json"), context)?;
        write_json(
            &self.run_dir.join("commands.json"),
            &vec![CommandTrace {
                tool: "vtbench".to_string(),
                cwd: metadata.cwd.display().to_string(),
                argv: std::iter::once(report.mode.clone())
                    .chain(metadata.raw_args.iter().cloned())
                    .collect(),
            }],
        )?;
        write_json(&self.run_dir.join("decision.json"), decision)?;
        write_json(
            &self.run_dir.join("env.json"),
            &EnvironmentManifest {
                cwd: metadata.cwd.display().to_string(),
                os: env::consts::OS.to_string(),
                arch: env::consts::ARCH.to_string(),
                cpu_count: std::thread::available_parallelism()
                    .map(usize::from)
                    .unwrap_or(1),
                git_sha: metadata.git.sha.clone(),
                dirty_tree: metadata.git.dirty_tree,
                env: capture_env_snapshot(),
            },
        )?;
        fs::write(self.run_dir.join("stdout.log"), metrics_text)
            .with_context(|| format!("write {}", self.run_dir.join("stdout.log").display()))?;
        fs::write(self.run_dir.join("stderr.log"), b"")
            .with_context(|| format!("write {}", self.run_dir.join("stderr.log").display()))?;
        fs::write(self.run_dir.join("metrics.txt"), metrics_text)
            .with_context(|| format!("write {}", self.run_dir.join("metrics.txt").display()))?;
        fs::write(self.run_dir.join("rerun.sh"), build_rerun_script(metadata, report))
            .with_context(|| format!("write {}", self.run_dir.join("rerun.sh").display()))?;
        let targets_dir = self.run_dir.join("targets");
        fs::create_dir_all(&targets_dir)
            .with_context(|| format!("create {}", targets_dir.display()))?;
        write_json(&targets_dir.join("primary.json"), report)?;
        Ok(())
    }
}

pub fn manifest_files() -> ManifestFiles {
    ManifestFiles {
        report: "report.json".to_string(),
        env: "env.json".to_string(),
        context: "context.json".to_string(),
        commands: "commands.json".to_string(),
        decision: "decision.json".to_string(),
        rerun: "rerun.sh".to_string(),
        stdout: "stdout.log".to_string(),
        stderr: "stderr.log".to_string(),
        metrics: "metrics.txt".to_string(),
    }
}

fn capture_git_metadata(cwd: &Path) -> Result<GitMetadata> {
    let sha = git_output(cwd, &["rev-parse", "HEAD"])?;
    let dirty_tree = Command::new("git")
        .arg("status")
        .arg("--porcelain")
        .arg("--untracked-files=no")
        .current_dir(cwd)
        .output()
        .map(|output| !String::from_utf8_lossy(&output.stdout).trim().is_empty())
        .unwrap_or(false);
    Ok(GitMetadata { sha, dirty_tree })
}

fn git_output(cwd: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("run git {}", args.join(" ")))?;
    if !output.status.success() {
        anyhow::bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn build_replay_id(mode: &str) -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis();
    let sanitized = mode.replace(|ch: char| !ch.is_ascii_alphanumeric(), "-");
    format!("{sanitized}-{millis}-{}", std::process::id())
}

fn build_command_display(executable_path: &Path, mode: &str, raw_args: &[String]) -> String {
    std::iter::once(executable_path.display().to_string())
        .chain(std::iter::once(mode.to_string()))
        .chain(raw_args.iter().cloned())
        .map(|arg| shell_escape(&arg))
        .collect::<Vec<_>>()
        .join(" ")
}

fn build_rerun_script(metadata: &ReplayMetadata, report: &CanonicalRunReport) -> String {
    format!(
        "#!/usr/bin/env bash\nset -euo pipefail\ncd {}\n{}\n",
        shell_escape(&metadata.cwd.display().to_string()),
        report.command_display
    )
}

fn capture_env_snapshot() -> BTreeMap<String, String> {
    env::vars()
        .filter(|(key, _)| {
            key.starts_with("VT_")
                || key.starts_with("CARGO_")
                || key == "PATH"
                || key == "RUST_LOG"
        })
        .collect()
}

fn shell_escape(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':' | '=' | ','))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let payload = serde_json::to_vec_pretty(value)?;
    fs::write(path, payload).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}
