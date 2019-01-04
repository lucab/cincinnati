//! Caching layer for tags and release metadata.

use actix::prelude::*;
use registry;
use std::collections::{BTreeMap, HashSet};

/// Request: populate release metadata for a tag.
pub(crate) struct Insert {
    /// Tag name.
    pub(crate) tag: String,
    /// Release metadata.
    pub(crate) release: Option<registry::Release>,
}

impl Message for Insert {
    type Result = ();
}

/// Request: lookup release metadata from cache, by tag.
pub(crate) struct Query {
    /// Tag name.
    pub(crate) tag: String,
}

impl Message for Query {
    type Result = Cached;
}

/// Reply: cached release metadata for a tag.
pub(crate) enum Cached {
    VacantEntry,
    KnownTag(Option<registry::Release>),
}

impl<A, M> dev::MessageResponse<A, M> for Cached
where
    A: Actor,
    M: Message<Result = Cached>,
{
    fn handle<R: dev::ResponseChannel<M>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self);
        }
    }
}

/// Cache management actor.
#[derive(Debug, Default)]
pub(crate) struct CacheManager {
    /// Cached negative results (tags without release metadata).
    no_release: HashSet<String>,
    /// Cached release metadata (by tag).
    tagged_release: BTreeMap<String, registry::Release>,
}

impl Actor for CacheManager {
    type Context = Context<Self>;
}

impl Supervised for CacheManager {}
impl SystemService for CacheManager {}

/// Cache querying.
impl Handler<Query> for CacheManager {
    type Result = Cached;

    fn handle(&mut self, msg: Query, _ctx: &mut Self::Context) -> Self::Result {
        if self.no_release.contains(&msg.tag) {
            return Cached::KnownTag(None);
        }

        if let Some(release) = self.tagged_release.get(&msg.tag) {
            return Cached::KnownTag(Some(release.clone()));
        }

        Cached::VacantEntry
    }
}

/// Cache insertion.
impl Handler<Insert> for CacheManager {
    type Result = ();

    fn handle(&mut self, msg: Insert, _ctx: &mut Self::Context) -> Self::Result {
        println!("insert: {} -> {:?}", msg.tag, msg.release);
        match msg.release {
            None => {
                self.no_release.insert(msg.tag);
            }
            Some(rel) => {
                self.tagged_release.insert(msg.tag, rel);
            }
        };
    }
}
