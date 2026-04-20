use crate::backend::QueryExecutor;
use crate::canonical::CheckpointVersionHeadFact;
use crate::{LixBackend, LixError};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    crate::streams::init(backend).await?;
    super::version_ops::init(backend).await?;
    super::checkpoint_ops::init(backend).await?;
    super::workspace::init(backend).await
}

pub(crate) async fn load_checkpoint_version_heads_for_init(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<CheckpointVersionHeadFact>, LixError> {
    Ok(
        super::version_ops::descriptors::load_checkpoint_version_heads_with_executor(executor)
            .await?
            .into_iter()
            .map(|head| CheckpointVersionHeadFact {
                version_id: head.version_id,
                head_commit_id: head.head_commit_id,
            })
            .collect(),
    )
}
