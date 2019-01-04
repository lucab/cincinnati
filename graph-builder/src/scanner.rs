//! Docker registry scanner.

use actix::prelude::*;
use cache;
use dkregistry;
use futures::future;
use futures::prelude::*;
use registry;
use std::time::Duration;
use tokio_core::reactor::Core;

/// Maximum number of parallel repo scans.
const MAX_REPO_SCANS: usize = 5;

/// Request: start scanning a repository.
pub(crate) struct ScanRepo {}

impl Message for ScanRepo {
    type Result = ();
}

/// Registry scanning actor.
#[derive(Debug)]
pub(crate) struct RegistryScanner {
    period: Duration,
    repository: RepoConfig,
    scans: Vec<Addr<RepoScanner>>,
}

/// Repository location and authentication info.
#[derive(Clone, Debug)]
struct RepoConfig {
    hostname: String,
    repository: String,
    username: Option<String>,
    password: Option<String>,
}

impl RegistryScanner {
    pub fn new(
        period: Duration,
        registry: String,
        repository: String,
        username: Option<String>,
        password: Option<String>,
    ) -> Self {
        let repo_info = RepoConfig {
            hostname: registry::trim_protocol(&registry).to_string(),
            repository,
            username,
            password,
        };

        Self {
            period,
            repository: repo_info,
            scans: vec![],
        }
    }
}

impl Actor for RegistryScanner {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.notify(ScanRepo {});
        ctx.run_interval(self.period, |_act, ctx| ctx.notify(ScanRepo {}));
    }
}

impl Handler<ScanRepo> for RegistryScanner {
    type Result = ();

    fn handle(&mut self, _msg: ScanRepo, ctx: &mut Self::Context) -> Self::Result {
        // Keep a limited number of parallel scans.
        if self.scans.len() >= MAX_REPO_SCANS {
            let oldest = self.scans.pop().expect("non-fallible pop");
            drop(oldest)
        }

        // Start a new scan, and record its address for cancellation.
        let scan = RepoScanner {
            config: self.repository.clone(),
        };
        let addr = scan.start();
        self.scans.push(addr);
    }
}

#[derive(Debug)]
struct RepoScanner {
    config: RepoConfig,
}

impl RepoScanner {
    /// Start scanning a repository.
    fn scan(repo: &RepoConfig) -> impl Future<Item = (), Error = ()> {
        let core = Core::new().unwrap();
        let repo_name = repo.repository.clone();
        let login_scope = format!("repository:{}:pull", &repo.repository);
        println!("registry {}", &repo.hostname);

        let client = dkregistry::v2::Client::configure(&core.handle())
            .registry(&repo.hostname)
            .insecure_registry(false)
            .build()
            .map_err(|e| format_err!("{}", e));
        future::result(client)
            .inspect(|_| println!("repo scan"))
            .and_then(move |client| {
                registry::authenticate_client2(client, login_scope)
                    .map_err(|e| format_err!("{}", e))
            })
            .map(move |client| (client, repo_name))
            .and_then(|(client, repo)| {
                let tags_stream = registry::fetch_tags(client, &repo);
                future::ok(tags_stream)
            })
            .flatten_stream()
            .for_each(|(_client, tag)| {
                println!("tag {}", tag);
                future::ok(())
            })
            .and_then(|_| {
                let act = actix::System::current()
                    .registry()
                    .get::<cache::CacheManager>();
                act.send(cache::Insert {
                    tag: "foo".into(),
                    release: None,
                })
                .from_err()
            })
            .map(|_| ())
            .map_err(|e| println!("scan error: {}", e))
    }
}

impl Actor for RepoScanner {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start a repository scan which auto-cancels
        // when the actor is dropped.
        let scan = Self::scan(&self.config);
        ctx.spawn(actix::fut::wrap_future(scan));
    }
}
