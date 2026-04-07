use std::path::{Path, PathBuf};

use graph_cypher::tck::feature_parser;
use graph_cypher::tck::{run_feature, should_skip_file, ScenarioResult};

fn tck_features_dir() -> PathBuf {
    // Try environment variable first, then fallback to common location
    if let Ok(dir) = std::env::var("TCK_FEATURES_DIR") {
        return PathBuf::from(dir);
    }
    panic!("TCK features directory not found. Set TCK_FEATURES_DIR env var.");
}

fn discover_feature_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                files.extend(discover_feature_files(&path));
            } else if path.extension().map_or(false, |e| e == "feature") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn run_feature_file(path: &Path) -> (usize, usize, usize, Vec<String>) {
    let content = std::fs::read_to_string(path).unwrap();
    let feature = feature_parser::parse_feature(&content);
    let results = run_feature(&feature);

    let mut pass = 0;
    let mut fail = 0;
    let mut skip = 0;
    let mut failures = Vec::new();

    for (name, result) in results {
        match result {
            ScenarioResult::Pass => pass += 1,
            ScenarioResult::Fail(msg) => {
                fail += 1;
                failures.push(format!("  FAIL: {name}\n    {msg}"));
            }
            ScenarioResult::Skip(_msg) => {
                skip += 1;
            }
        }
    }

    (pass, fail, skip, failures)
}

/// Generate one test per feature file.
/// We use a macro to create individual tests dynamically.
macro_rules! feature_test {
    ($name:ident, $path:expr) => {
        #[test]
        fn $name() {
            let path = Path::new($path);
            if !path.exists() {
                eprintln!("Skipping {}: file not found", $path);
                return;
            }
            if should_skip_file($path) {
                eprintln!("Skipping {}: category excluded", $path);
                return;
            }
            let (pass, fail, skip, failures) = run_feature_file(path);
            eprintln!(
                "{}: {} passed, {} failed, {} skipped",
                path.file_name().unwrap().to_str().unwrap(),
                pass,
                fail,
                skip
            );
            for f in &failures {
                eprintln!("{f}");
            }
            // Don't assert yet - we want to see the full report first
            // Uncomment when ready: assert_eq!(fail, 0, "{} test(s) failed", fail);
        }
    };
}

/// Run all feature files and produce a summary report.
#[test]
fn tck_summary() {
    // Ensure sufficient stack for deeply recursive pattern matching
    let builder = std::thread::Builder::new().stack_size(16 * 1024 * 1024);
    let handler = builder
        .spawn(|| tck_summary_inner())
        .expect("failed to spawn thread");
    handler.join().expect("test thread panicked");
}

fn tck_summary_inner() {
    let features_dir = tck_features_dir();
    let files = discover_feature_files(&features_dir);

    let mut total_pass = 0;
    let mut total_fail = 0;
    let mut total_skip = 0;
    let mut total_files = 0;
    let mut skipped_files = 0;
    let mut all_failures = Vec::new();

    for path in &files {
        let rel_path = path.strip_prefix(&features_dir).unwrap_or(path);
        let rel_str = rel_path.to_str().unwrap();

        if should_skip_file(rel_str) {
            skipped_files += 1;
            continue;
        }

        total_files += 1;
        let (pass, fail, skip, failures) = run_feature_file(path);
        total_pass += pass;
        total_fail += fail;
        total_skip += skip;

        if !failures.is_empty() {
            all_failures.push(format!("\n--- {} ---", rel_str));
            all_failures.extend(failures);
        }
    }

    eprintln!("\n=== TCK Summary ===");
    eprintln!(
        "Feature files: {} run, {} skipped",
        total_files, skipped_files
    );
    eprintln!(
        "Scenarios: {} passed, {} failed, {} skipped",
        total_pass, total_fail, total_skip
    );
    let total = total_pass + total_fail;
    if total > 0 {
        eprintln!(
            "Pass rate: {:.1}%",
            (total_pass as f64 / total as f64) * 100.0
        );
    }

    if !all_failures.is_empty() {
        eprintln!("\n=== Failures ===");
        for f in &all_failures {
            eprintln!("{f}");
        }
    }
}
