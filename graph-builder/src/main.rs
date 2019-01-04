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
extern crate cincinnati;
extern crate dkregistry;
extern crate env_logger;
extern crate itertools;
#[macro_use]
extern crate failure;
extern crate flate2;
extern crate futures;
#[macro_use]
extern crate log;
extern crate reqwest;
extern crate semver;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate structopt;
extern crate tar;
extern crate tokio;
extern crate tokio_core;

mod cache;
mod config;
mod graph;
mod registry;
mod release;
mod scanner;

use actix::{Actor, Arbiter};
use actix_web::{http::Method, middleware::Logger, server, App};
use failure::Error;
use log::LevelFilter;
use std::thread;
use structopt::StructOpt;

fn main() -> Result<(), Error> {
    let sys = actix::System::new("graph-builder");
    let opts = config::Options::from_args();
    let addr = (opts.address, opts.port);

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
    /*
        {
            let state = state.clone();
            thread::spawn(move || graph::run(&opts, &state));
        }
    */
    // Registry scanning, in a dedicated thread.
    let _scaner = {
        let (user, passwd) =
            registry::read_credentials(opts.credentials_path.as_ref(), &opts.registry)?;
        let actor = scanner::RegistryScanner::new(
            opts.period.clone(),
            opts.registry.clone(),
            opts.repository.clone(),
            user,
            passwd,
        );
        Arbiter::start(|_| actor)
    };

    // Release metadata caching, in a dedicated thread.
    let _cache = Arbiter::start(|_| cache::CacheManager::default());

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
