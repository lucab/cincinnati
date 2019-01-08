//! Caching layer for tags and release metadata.

use actix::prelude::*;
use registry;
use std::collections::HashMap;

/// Cache management actor.
#[derive(Debug, Default)]
pub struct CacheManager {
    cache: HashMap<u64, Option<registry::Release>>,
}

impl Actor for CacheManager {
    type Context = Context<Self>;
}

impl Supervised for CacheManager {}
impl SystemService for CacheManager {}

/// Request: query for a tagged release (by hash).
///
/// Update Images with release metadata should be immutable, but
/// tags on registry can be mutated at any time. Thus, the cache
/// is keyed on the hash of tag layers.
pub(crate) struct QueryCache(pub(crate) u64);

impl Message for QueryCache {
    type Result = Option<Option<registry::Release>>;
}

impl Handler<QueryCache> for CacheManager {
    type Result = Option<Option<registry::Release>>;

    fn handle(&mut self, msg: QueryCache, _ctx: &mut Self::Context) -> Self::Result {
        println!("cache query {}", msg.0);
        let hashed_tag_layers = &msg.0;
        self.cache.get(hashed_tag_layers).cloned()
    }
}

/// Request: cache a tagged release (by hash).
///
/// Each tagged release is looked up at most once and both
/// positive (Some metadata) and negative (None) results cached
/// indefinitely.
pub(crate) struct UpdateCache {
    pub(crate) tag_hash: u64,
    pub(crate) release: Option<registry::Release>,
}

impl Message for UpdateCache {
    type Result = ();
}

impl Handler<UpdateCache> for CacheManager {
    type Result = ();

    fn handle(&mut self, msg: UpdateCache, _ctx: &mut Self::Context) -> Self::Result {
        if self.cache.insert(msg.tag_hash, msg.release).is_none() {
            trace!("cached new release with hashed tag {}", msg.tag_hash);
        };
    }
}
