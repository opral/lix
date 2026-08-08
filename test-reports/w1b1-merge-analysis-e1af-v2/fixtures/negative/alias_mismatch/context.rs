pub(crate) fn forktree_read_facade(&self) -> ForkTreeReadFacade<OpeningRead> {
    let read = self.opening_read();
    ForkTreeReadFacade::new(read)
}

