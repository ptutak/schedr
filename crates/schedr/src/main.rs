//! The `schedr` executable: reads the process environment and files, then runs
//! [`schedr_bin::run`].

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::process::ExitCode;

fn main() -> ExitCode {
    let env: BTreeMap<String, String> = std::env::vars().collect();
    let code = schedr_bin::run(
        std::env::args_os(),
        &env,
        |path| std::fs::read_to_string(path),
        std::io::stdout().is_terminal(),
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    );
    ExitCode::from(code)
}
