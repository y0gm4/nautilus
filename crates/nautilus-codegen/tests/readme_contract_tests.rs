#[test]
fn readme_tracks_generated_client_install_story() {
    let readme = std::fs::read_to_string(format!("{}/README.md", env!("CARGO_MANIFEST_DIR")))
        .expect("failed to read codegen README");

    assert!(readme.contains("import the generated `output` package directly"));
    assert!(readme.contains("site-packages/nautilus"));
    assert!(readme.contains("not a PyPI publish step"));
    assert!(readme.contains("node_modules/nautilus"));
    assert!(readme.contains("not an npm publish step"));
    assert!(readme.contains("nautilus-client-java"));
    assert!(readme.contains("Maven module"));
    assert!(readme.contains("mode = \"jar\""));
    assert!(readme.contains("`install = true` is ignored"));
    assert!(readme.contains(".nautilus-build"));
    assert!(readme.contains("javac --release 21"));
    assert!(readme.contains("The checked-in examples show the intended consumption pattern today"));
    assert!(readme.contains("Choosing `findMany` vs streaming APIs"));
    assert!(readme.contains("Rust async"));
    assert!(readme.contains("streamMany"));
    assert!(readme.contains("stream_many"));
}

#[test]
fn workspace_readme_documents_buffered_vs_streaming_reads() {
    let readme = std::fs::read_to_string(format!("{}/../../README.md", env!("CARGO_MANIFEST_DIR")))
        .expect("failed to read workspace README");

    assert!(readme.contains("### Streaming Reads"));
    assert!(readme.contains("Use `findMany` / `find_many`"));
    assert!(readme.contains("`stream_many(...) -> AsyncIterator[Model]`"));
    assert!(readme.contains("`streamMany(...) -> AsyncIterable<Model>`"));
    assert!(readme.contains("`streamMany(...) -> Stream<Model>`"));
    assert!(readme.contains("`stream_many(...) -> Result<impl Stream<...>>`"));
}
