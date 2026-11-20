//! A plugin in Rust: `cargo build --example hello-plugin -p schedr-extension`, then on a worker
//! `[extensions.hello] command = ["<target>/debug/examples/hello-plugin"]`, `versions = ["1.0"]`.
//! Its payload is `{"name": "..."}`; it greets, and returns the greeting.

use std::process::ExitCode;

use schedr_extension::{Channel, Plugin, Request, Value, json};

struct Hello;

impl Plugin for Hello {
    fn name(&self) -> &'static str {
        "hello"
    }

    fn versions(&self) -> Vec<String> {
        vec!["1.0".to_owned()]
    }

    fn run(&mut self, request: &Request, _channel: &mut Channel) -> Result<Value, String> {
        let name = request.payload["name"].as_str().unwrap_or("world");
        let greeting = format!("hello, {name}");
        println!("{greeting}");
        Ok(json!({ "greeting": greeting }))
    }
}

fn main() -> ExitCode {
    schedr_extension::main(Hello)
}
