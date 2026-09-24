//! Prospective actor cache state owned until durable transaction publication.
//!
//! Dropping this owner preserves the lease's cancellation retirement and the
//! Store-before-permit drop order. Orderly failures await `discard`; cache
//! publication is derived state and must happen only after durable success.

use super::{
    PluginActorCache, PluginActorDetachedSuccessor, PluginActorKey, PluginActorLease,
    PluginActorPendingPublication, PluginActorStagedCheckpoint, PluginActorStore,
    PluginObservation, PluginRowAuthorities, WasmDocumentCheckpoint, WasmDocumentHandle,
};
use crate::{Blob, LixError};
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(crate) struct PluginPublicationPolicy {
    pub(crate) semantic_chainable: bool,
    pub(crate) retain_large_import_actor: bool,
}

pub(crate) struct PendingPluginActorPublication {
    key: PluginActorKey,
    policy: PluginPublicationPolicy,
    state: PendingActorState,
    _publication_marker: Option<PluginActorPendingPublication>,
}

enum PendingActorState {
    Existing(PluginActorLease),
    Detached(PluginActorDetachedSuccessor),
    New {
        cache: PluginActorCache,
        store: PluginActorStore,
        document: WasmDocumentHandle,
        checkpoint: Option<WasmDocumentCheckpoint>,
        bytes: Blob,
        semantic_root: Arc<str>,
        row_authorities: PluginRowAuthorities,
    },
    Uncached(Option<PluginActorStagedCheckpoint>),
}

pub(crate) enum ChainablePublication {
    Chainable(PluginActorLease, PluginActorKey, PluginPublicationPolicy),
    Pending(PendingPluginActorPublication),
}

pub(crate) struct PluginPublicationReceipt {
    pub(crate) key: PluginActorKey,
    pub(crate) observation: Option<PluginObservation>,
}

impl PendingPluginActorPublication {
    pub(crate) fn existing(
        key: PluginActorKey,
        lease: PluginActorLease,
        policy: PluginPublicationPolicy,
    ) -> Self {
        Self {
            key,
            policy,
            state: PendingActorState::Existing(lease),
            _publication_marker: None,
        }
    }

    pub(crate) fn new(
        key: PluginActorKey,
        cache: PluginActorCache,
        store: PluginActorStore,
        document: WasmDocumentHandle,
        checkpoint: Option<WasmDocumentCheckpoint>,
        bytes: Blob,
        semantic_root: Arc<str>,
        row_authorities: PluginRowAuthorities,
        policy: PluginPublicationPolicy,
    ) -> Self {
        let publication_marker = cache.track_publication(&key);
        Self {
            key,
            policy,
            state: PendingActorState::New {
                cache,
                store,
                document,
                checkpoint,
                bytes,
                semantic_root,
                row_authorities,
            },
            _publication_marker: Some(publication_marker),
        }
    }

    pub(crate) fn key(&self) -> &PluginActorKey {
        &self.key
    }

    pub(crate) fn detach_lease(mut self) -> Self {
        self.state = match self.state {
            PendingActorState::Existing(lease) => {
                self._publication_marker = Some(lease.pending_publication_marker(&self.key));
                PendingActorState::Detached(lease.detach_successor())
            }
            state => state,
        };
        self
    }

    pub(crate) fn retains_large_import_actor(&self) -> bool {
        self.policy.retain_large_import_actor
    }

    pub(crate) async fn into_chainable(self, expected: &PluginActorKey) -> ChainablePublication {
        if self.key == *expected
            && self.policy.semantic_chainable
            && matches!(
                self.state,
                PendingActorState::Existing(_) | PendingActorState::Detached(_)
            )
        {
            let Self {
                key,
                policy,
                state,
                _publication_marker,
            } = self;
            match state {
                PendingActorState::Existing(lease) => {
                    ChainablePublication::Chainable(lease, key, policy)
                }
                PendingActorState::Detached(detached) => match detached.chain().await {
                    Ok(lease) => ChainablePublication::Chainable(lease, key, policy),
                    Err(_) => ChainablePublication::Pending(Self {
                        key,
                        policy,
                        state: PendingActorState::Uncached(None),
                        _publication_marker: None,
                    }),
                },
                _ => unreachable!(),
            }
        } else {
            ChainablePublication::Pending(self)
        }
    }

    pub(crate) async fn into_uncached(self) -> Self {
        let Self {
            key,
            policy,
            state,
            _publication_marker,
        } = self;
        let checkpoint = match state {
            PendingActorState::Existing(lease) => {
                let checkpoint =
                    lease
                        .successor_checkpoint()
                        .and_then(|(cache, root, checkpoint)| {
                            cache.stage_checkpoint(key.clone(), root, checkpoint)
                        });
                let _ = lease.discard_successor().await;
                checkpoint
            }
            PendingActorState::Detached(detached) => {
                let checkpoint =
                    detached
                        .successor_checkpoint()
                        .and_then(|(cache, root, checkpoint)| {
                            cache.stage_checkpoint(key.clone(), root, checkpoint)
                        });
                let _ = detached.discard().await;
                checkpoint
            }
            PendingActorState::New {
                cache,
                mut store,
                document,
                checkpoint,
                semantic_root,
                ..
            } => {
                let checkpoint = cache.stage_checkpoint(key.clone(), semantic_root, checkpoint);
                let _ = store.actor_mut().drop_document(document).await;
                let _ = store.actor_mut().retire().await;
                checkpoint
            }
            PendingActorState::Uncached(checkpoint) => checkpoint,
        };
        Self {
            key,
            policy,
            state: PendingActorState::Uncached(checkpoint),
            _publication_marker: None,
        }
    }

    pub(crate) async fn discard(self) {
        match self.state {
            PendingActorState::Existing(lease) => {
                let _ = lease.discard_successor().await;
            }
            PendingActorState::Detached(lease) => {
                let _ = lease.discard().await;
            }
            PendingActorState::New {
                mut store,
                document,
                ..
            } => {
                let _ = store.actor_mut().drop_document(document).await;
                let _ = store.actor_mut().retire().await;
            }
            PendingActorState::Uncached(_) => {}
        }
    }

    pub(crate) async fn publish(self) -> Result<PluginPublicationReceipt, LixError> {
        let observation = match self.state {
            PendingActorState::Existing(lease) => {
                Some(lease.commit_successor_as(self.key.clone()).await?)
            }
            PendingActorState::Detached(lease) => Some(lease.publish(self.key.clone()).await?),
            PendingActorState::New {
                cache,
                store,
                document,
                checkpoint,
                bytes,
                semantic_root,
                row_authorities,
            } => {
                cache.remember_checkpoint(&self.key, &semantic_root, checkpoint);
                Some(cache.install_with_authorities(
                    self.key.clone(),
                    store,
                    document,
                    bytes,
                    semantic_root,
                    row_authorities,
                ))
            }
            PendingActorState::Uncached(checkpoint) => {
                if let Some(checkpoint) = checkpoint {
                    checkpoint.publish();
                }
                None
            }
        };
        Ok(PluginPublicationReceipt {
            key: self.key,
            observation,
        })
    }
}

/// Release a completed Store while retaining its staged checkpoint and identity.
pub(crate) async fn retire_oldest_completed_actor(
    publications: &mut Vec<PendingPluginActorPublication>,
) -> bool {
    let Some(index) = publications
        .iter()
        .position(|publication| !matches!(publication.state, PendingActorState::Uncached(_)))
    else {
        return false;
    };
    let publication = publications.remove(index).into_uncached().await;
    publications.insert(index, publication);
    true
}

pub(crate) async fn discard_plugin_actor_publications(
    publications: Vec<PendingPluginActorPublication>,
) {
    for publication in publications {
        publication.discard().await;
    }
}
