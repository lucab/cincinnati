// Copyright 2018 Alex Crawford
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate actix;
extern crate actix_web;
extern crate failure;
extern crate graph_builder;
extern crate log;
extern crate structopt;

use graph_builder::{cache, config, graph, registry, registry_scanner};

use actix::prelude::*;
use actix_web::{http::Method, middleware::Logger, server, App};
use failure::Error;
use log::LevelFilter;
use structopt::StructOpt;

fn main() -> Result<(), Error> {
    let sys = actix::System::new("graph-builder");
    let opts = config::Options::from_args();

    env_logger::Builder::from_default_env()
        .filter(
            Some(module_path!()),
            match opts.verbosity {
                0 => LevelFilter::Warn,
                1 => LevelFilter::Info,
                2 => LevelFilter::Debug,
                _ => LevelFilter::Trace,
            },
        )
        .init();

    let state = graph::State::new();
    let addr = (opts.address, opts.port);

    // Release metadata caching, in a dedicated thread.
    let _cache = Arbiter::start(|_| cache::CacheManager::default());

    // Registry scanning, in a dedicated thread.
    let _scanner = {
        let (username, password) =
            registry::read_credentials(opts.credentials_path.as_ref(), &opts.registry)?;

        let actor =
            registry_scanner::RegistryScanner::new(state.clone(), opts.clone(), username, password);
        Arbiter::start(|_| actor)
    };

    // Graph rendering service.
    server::new(move || {
        App::with_state(state.clone())
            .middleware(Logger::default())
            .route("/v1/graph", Method::GET, graph::index)
    })
    .bind(addr)?
    .start();

    sys.run();
    Ok(())
}
