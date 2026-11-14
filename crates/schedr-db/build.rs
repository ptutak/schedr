//! Rebuilds the crate when a migration changes: `sqlx::migrate!` embeds the SQL files, and cargo
//! doesn't see them otherwise (a migration edited alone left the old one in the binary).

fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
