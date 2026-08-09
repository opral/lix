#[cfg(test)]
use async_trait::async_trait;

#[cfg(test)]
use crate::LixError;
#[cfg(test)]
use crate::binary_cas::BlobId;

#[cfg(test)]
#[async_trait]
pub(crate) trait BlobDataReader: Send + Sync {
    async fn load_bytes_many(
        &self,
        hashes: &[BlobId],
    ) -> Result<crate::binary_cas::BlobBytesBatch, LixError>;

    #[cfg(test)]
    async fn load_ranges_many(
        &self,
        requests: &[(BlobId, std::ops::Range<u64>)],
    ) -> Result<crate::binary_cas::BlobRangeBytesBatch, LixError> {
        let hashes = requests.iter().map(|(hash, _)| *hash).collect::<Vec<_>>();
        let values = self.load_bytes_many(&hashes).await?.into_vec();
        let entries = values
            .into_iter()
            .zip(requests)
            .map(|(value, (_, requested))| {
                value
                    .map(|bytes| materialize_blob_range(bytes, requested.clone()))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(crate::binary_cas::BlobRangeBytesBatch::new(entries))
    }
}

#[cfg(test)]
fn materialize_blob_range(
    bytes: Vec<u8>,
    requested: std::ops::Range<u64>,
) -> Result<crate::binary_cas::BlobRangeBytes, LixError> {
    let total_size = u64::try_from(bytes.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "binary CAS blob size exceeds u64",
        )
    })?;
    if requested.start >= requested.end || requested.start >= total_size {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "binary CAS range is not satisfiable",
        ));
    }
    let range = requested.start..requested.end.min(total_size);
    let start = usize::try_from(range.start).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "binary CAS range is too large",
        )
    })?;
    let end = usize::try_from(range.end).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "binary CAS range is too large",
        )
    })?;
    Ok(crate::binary_cas::BlobRangeBytes {
        bytes: bytes[start..end].to_vec(),
        total_size,
        range,
    })
}
