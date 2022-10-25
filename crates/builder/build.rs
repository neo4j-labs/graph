use std::env;
use std::fs;
use std::path::Path;
use std::process::{Command, ExitStatus};
use std::str;

// Checks if the maybe_uninit_write_slice feature can be enabled
const MAYBE_UNINIT_WRITE_SLICE_PROBE: &str = r#"
#![feature(maybe_uninit_write_slice)]
#![allow(dead_code)]

use std::mem::MaybeUninit;

pub fn test() {
    MaybeUninit::write_slice(
        [MaybeUninit::<usize>::uninit()].as_mut_slice(),
        [1_usize].as_slice()
    );
}
"#;

fn main() {
    match compile_probe(MAYBE_UNINIT_WRITE_SLICE_PROBE) {
        Some(status) if status.success() => {}
        _ => println!("cargo:rustc-cfg=no_maybe_uninit_write_slice"),
    }
}

// Checks if some code can be compiled with the current toolchain
fn compile_probe(probe: &str) -> Option<ExitStatus> {
    let rustc = env::var_os("RUSTC")?;
    let out_dir = env::var_os("OUT_DIR")?;
    let probefile = Path::new(&out_dir).join("probe.rs");
    fs::write(&probefile, probe).ok()?;
    Command::new(rustc)
        .arg("--edition=2021")
        .arg("--crate-name=graph_build")
        .arg("--crate-type=lib")
        .arg("--emit=metadata")
        .arg("--out-dir")
        .arg(out_dir)
        .arg(probefile)
        .status()
        .ok()
}
