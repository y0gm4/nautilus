//! Java code generator module.

pub mod backend;
pub mod bundle;
pub mod generator;
pub mod type_mapper;

pub use backend::JavaBackend;
pub use bundle::build_java_bundle;
pub use generator::generate_java_client;
