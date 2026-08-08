struct CoherentView;
struct ForkTreeReadFacade<'a> { forktree_reader: &'a CoherentView }

fn provider(source: &ForkTreeReadFacade<'_>) {
    let reader = source.forktree_reader;
    read_point(reader, reader);
}

fn read_point(view: &CoherentView, same_view: &CoherentView) {
    let _ = (view, same_view);
}
