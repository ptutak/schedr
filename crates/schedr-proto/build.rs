//! Compiles the protocol's `.proto` files with a vendored `protoc`, so building needs nothing
//! installed.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(
        config,
        &["proto/schedr/protocol/v1/master.proto"],
        &["proto"],
    )?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
