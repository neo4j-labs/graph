fn main() {
    // nanorand (via graph_builder) calls RtlGenRandom (SystemFunction036) but does
    // not declare the advapi32 dependency itself; Rust no longer links it implicitly.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rustc-link-lib=advapi32");
    }
}
