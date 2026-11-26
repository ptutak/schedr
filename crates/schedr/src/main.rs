//! The `schedr` executable: reads the process environment and files, then runs
//! [`schedr_bin::run`].

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::process::ExitCode;

fn main() -> ExitCode {
    // Not `vars()`: a variable that isn't UTF-8 panicked it (agy's review of M5.3c).
    let env: BTreeMap<String, String> = schedr_bin::ctx::env_from(std::env::vars_os());
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
