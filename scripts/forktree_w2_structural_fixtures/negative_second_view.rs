struct CoherentView;
struct Storage;

fn provider(storage: &mut Storage) {
    let first = storage.begin_read();
    let second = storage.begin_read();
    consume(first, second);
}

fn consume(_: CoherentView, _: CoherentView) {}
