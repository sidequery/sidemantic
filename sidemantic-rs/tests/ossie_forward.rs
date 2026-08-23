use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sidemantic::{OssieConsumerProfile, OssieForwardAdapter, OssieSerialization};

#[derive(Debug, Deserialize)]
struct Manifest {
    cases: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
struct FixtureCase {
    id: String,
    input: String,
    serialization: String,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
struct Expected {
    valid: bool,
    #[serde(default)]
    diagnostics: Vec<ExpectedDiagnostic>,
}

#[derive(Debug, Deserialize)]
struct ExpectedDiagnostic {
    code: String,
    instance_path: String,
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("tests/ossie-fixtures")
}

#[test]
fn shared_ossie_fixture_manifest_matches_strict_rust_diagnostics() {
    let root = fixture_root();
    let manifest: Manifest =
        serde_yaml::from_str(&fs::read_to_string(root.join("manifest.yaml")).unwrap()).unwrap();

    for case in manifest.cases {
        let serialization = OssieSerialization::parse(&case.serialization).unwrap();
        let content = fs::read_to_string(root.join(&case.input)).unwrap();
        let status =
            OssieForwardAdapter.inspect(&content, serialization, OssieConsumerProfile::OssieCore);

        assert_eq!(status.valid, case.expected.valid, "fixture {}", case.id);
        for expected in case.expected.diagnostics {
            assert!(
                status.diagnostics.iter().any(|actual| {
                    actual.code == expected.code && actual.instance_path == expected.instance_path
                }),
                "fixture {} missing diagnostic {} at {}; actual: {:?}",
                case.id,
                expected.code,
                expected.instance_path,
                status.diagnostics
            );
        }
    }
}
