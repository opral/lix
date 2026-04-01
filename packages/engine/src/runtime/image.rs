use async_trait::async_trait;

use crate::LixError;

#[async_trait(?Send)]
pub trait ImageChunkReader: Send {
    async fn read_chunk(&mut self) -> Result<Option<Vec<u8>>, LixError>;
}

#[async_trait(?Send)]
pub trait ImageChunkWriter: Send {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), LixError>;

    async fn finish(&mut self) -> Result<(), LixError> {
        Ok(())
    }
}
