pub(crate) struct ForkTreeReadFacade<R> { read: R }
impl<R> ForkTreeReadFacade<R> {
    async fn branch(&self, id: &str) -> Result<CoherentView<&R>, Error> { todo!() }
}

