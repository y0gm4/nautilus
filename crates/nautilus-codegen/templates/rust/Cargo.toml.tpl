[package]
name = "nautilus-client"
version = "0.1.0"
edition = "2021"

[workspace]

[lib]
path = "src/lib.rs"

[dependencies]
nautilus-core = { path = "{{ workspace_root_path }}/crates/nautilus-core", package = "nautilus-orm-core" }
nautilus-connector = { path = "{{ workspace_root_path }}/crates/nautilus-connector", package = "nautilus-orm-connector" }
nautilus-dialect = { path = "{{ workspace_root_path }}/crates/nautilus-dialect", package = "nautilus-orm-dialect" }
nautilus-engine = { path = "{{ workspace_root_path }}/crates/nautilus-engine", package = "nautilus-orm-engine" }
nautilus-protocol = { path = "{{ workspace_root_path }}/crates/nautilus-protocol", package = "nautilus-orm-protocol" }
nautilus-schema = { path = "{{ workspace_root_path }}/crates/nautilus-schema", package = "nautilus-orm-schema" }

tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
futures = "0.3"
async-stream = "0.3"

serde = { version = "1", features = ["derive"] }
serde_json = "1.0"

chrono = { version = "0.4", default-features = false, features = ["std"] }
uuid = { version = "1.0", features = ["v4"] }
rust_decimal = "1.35"
