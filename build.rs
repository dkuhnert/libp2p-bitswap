use pb_rs::{types::FileDescriptor, ConfigBuilder};

fn main() {
    let in_files: Vec<&str> = vec!["src/compat/pb/bitswap_pb.proto"];
    for in_file in in_files.iter() {
        println!("cargo:rerun-if-changed={}", in_file);
    }
    let config_builder = ConfigBuilder::new(&in_files, None, None, &[]).unwrap();
    FileDescriptor::run(&config_builder.build()).unwrap()
}
