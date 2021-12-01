#[test]
fn basic_functionality() {
    let mut log = Log::new("basic_functionality").unwrap();

    let mut log2 = log.new_shard();
}
