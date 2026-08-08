pub(crate) struct ForkTreeReadFacade<R> { read: R }

impl<R> ForkTreeReadFacade<R> {
    pub(crate) async fn branch(&self, id: &str) -> Result<CoherentView<&R>, Error> {
        open_coherent_view_on_read(&self.read, id).await
    }
}

