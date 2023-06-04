use proto_builder_trait::tonic::BuilderAttributes;
use std::process::Command;

fn main() {
    tonic_build::configure()
        .out_dir("src/grpc/pb")
        .with_serde(
            &["store.Msg", "store.MsgId", "store.MsgTime"],
            true,
            true,
            Some(&[r#"#[serde(rename_all = "camelCase")]"#]),
        )
        .with_derive_builder(
            &["store.Msg", "store.MsgId", "store.MsgTime"],
            Some(&[r#"#[builder(build_fn(name = "private_build"))]"#]),
        )
        .compile(&["protos/store.proto"], &["protos"])
        .unwrap();

    // fs::remove_file("grpc/pb/google.protobuf.rs").unwrap();
    Command::new("cargo").args(["fmt"]).output().unwrap();

    println!("cargo:rerun-if-changed=protos/store.proto");
}
