//! The small `schedr-ctx` executable (M5.3c): `schedr task-ctx`'s commands alone, linking only
//! `schedr_ctx`, so a task's call starts in a fraction of `schedr`'s time. Beside `schedr` in the
//! wheel and the images.

use std::collections::BTreeMap;
use std::process::ExitCode;

fn main() -> ExitCode {
    let env: BTreeMap<String, String> = schedr_ctx::env_from(std::env::vars_os());
    ExitCode::from(schedr_ctx::run(
        std::env::args_os(),
        &env,
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    ))
}
