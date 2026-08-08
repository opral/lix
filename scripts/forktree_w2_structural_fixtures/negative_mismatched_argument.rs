struct CoherentView;

fn provider(first: &CoherentView, second: &CoherentView) {
    read_point(first, second);
}

fn read_point(expected: &CoherentView, actual: &CoherentView) {
    let _ = (expected, actual);
}
