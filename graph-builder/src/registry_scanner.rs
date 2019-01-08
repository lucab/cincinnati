//! Scan a repository on a docker v2 registry for release metadata.

use actix::prelude::*;
use cache;
use config;
use graph;
use registry;

/// Scanner actor for docker-registry v2.
pub struct RegistryScanner {
    state: graph::State,
    opts: config::Options,
    username: Option<String>,
    password: Option<String>,
}

impl RegistryScanner {
    /// Create a new scanner.
    pub fn new(
        state: graph::State,
        opts: config::Options,
        username: Option<String>,
        password: Option<String>,
    ) -> Self {
        Self {
            state,
            opts,
            username,
            password,
        }
    }

    /// Update graph state.
    fn update_state(&mut self, releases: Vec<registry::Release>) {
        trace!("updating graph, {} known releases", releases.len());

        // TODO(lucab): do not lock the graph. Instead, investigate moving
        // ownership to the rendering service and send async updates to it.
        match graph::create_graph(&self.opts, releases) {
            Ok(graph) => match serde_json::to_string(&graph) {
                Ok(json) => {
                    *self
                        .state
                        .json
                        .write()
                        .expect("json lock has been poisoned") = json
                }
                Err(err) => error!("Failed to serialize graph: {}", err),
            },
            Err(err) => err.iter_chain().for_each(|cause| error!("{}", cause)),
        }
    }
}

impl Actor for RegistryScanner {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(
            "started scanner for repository {} on {}",
            self.opts.repository, self.opts.registry
        );
        ctx.notify(ScanRepo {});
        ctx.run_interval(self.opts.period, |_act, ctx| ctx.notify(ScanRepo {}));
    }
}

/// Request: start scanning a repository.
struct ScanRepo {}

impl Message for ScanRepo {
    type Result = ();
}

impl Handler<ScanRepo> for RegistryScanner {
    type Result = ();

    fn handle(&mut self, _msg: ScanRepo, ctx: &mut Self::Context) -> Self::Result {
        use futures::future;
        use futures::future::Either;
        use futures::prelude::*;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use tokio_core::reactor::Core;
        trace!("repository scan triggered");

        /*
        let scan = registry::fetch_releases(
            self.opts.registry.clone(),
            self.opts.repository.clone(),
            self.username.clone(),
            self.password.clone(),
        );
        */
        let registry_host = registry::trim_protocol(&self.opts.registry);
        let login_scope = format!("repository:{}:pull", &self.opts.repository);
        let repo = self.opts.repository.clone();

        let client = dkregistry::v2::Client::configure(&Core::new().unwrap().handle())
            .registry(registry_host)
            .insecure_registry(false)
            .username(self.username.clone())
            .password(self.password.clone())
            .build()
            .map_err(|e| format_err!("{}", e));

        let host = registry_host.to_string();
        let fetch_releases = future::result(client)
            .map(move |client| (client, login_scope))
            .and_then(|(client, scope)| registry::authenticate_client(client, scope))
            .and_then(|authenticated_client| {
                let tags_stream = registry::get_tags(repo, authenticated_client);
                future::ok(tags_stream)
            })
            .flatten_stream()
            .and_then(|(authenticated_client, repo, tag)| {
                registry::get_manifest_and_layers(tag, repo, authenticated_client)
            })
            .and_then(|(authenticated_client, repo, tag, digests)| {
                let hashed_tag_layers = {
                    let mut hasher = DefaultHasher::new();
                    digests.hash(&mut hasher);
                    hasher.finish()
                };

                let act = actix::System::current()
                    .registry()
                    .get::<cache::CacheManager>();
                act.send(cache::QueryCache(hashed_tag_layers))
                    .from_err()
                    .map(move |cached| (authenticated_client, repo, tag, digests, cached))
            })
            .and_then(move |(client, repo, tag, digests, cached)| {
                if let Some(release) = cached {
                    return Either::A(future::ok((digests, release)));
                }

                Either::B(registry::find_first_release(
                    digests,
                    client,
                    host.clone(),
                    repo,
                    tag,
                ))
            })
            .and_then(|(digests, release)| {
                let tag_hash = {
                    let mut hasher = DefaultHasher::new();
                    digests.hash(&mut hasher);
                    hasher.finish()
                };

                let update = cache::UpdateCache {
                    tag_hash,
                    release: release.clone(),
                };
                let act = actix::System::current()
                    .registry()
                    .get::<cache::CacheManager>();
                act.send(update).from_err().map(move |_| release)
            })
            .filter_map(|release| release)
            .collect();

        let async_update = actix::fut::wrap_future::<_, Self>(fetch_releases)
            .map(|releases, actor, _ctx| {
                actor.update_state(releases);
            })
            .map_err(|e, _act, _ctx| error!("{}", e));

        // TODO(lucab): add timeouts and limit the number of parallel scans.
        ctx.spawn(async_update);
    }
}
