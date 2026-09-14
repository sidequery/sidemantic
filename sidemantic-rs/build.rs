fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var("TARGET").as_deref() == Ok("wasm32-unknown-unknown") {
        // Native semantic entrypoints run the parser and recursive AST
        // serialization on a 16 MiB worker stack. WASM cannot spawn that worker;
        // reserve the same stack in linear memory for both the exported cdylib
        // (cargo/wasm-pack) and the integration-test executable. Input work
        // limits remain independently enforced by wasm_sql_guard.
        const STACK_SIZE: usize = 16 * 1024 * 1024;
        println!("cargo:rustc-link-arg=-zstack-size={STACK_SIZE}");
    }
}
