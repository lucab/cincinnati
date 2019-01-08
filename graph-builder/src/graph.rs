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

use actix_web::http::header::{self, HeaderValue};
use actix_web::{HttpMessage, HttpRequest, HttpResponse};
use cincinnati::{AbstractRelease, Graph, Release, CONTENT_TYPE};
use config;
use failure::Error;
use registry;
use std::sync::{Arc, RwLock};

pub fn index(req: HttpRequest<State>) -> HttpResponse {
    match req.headers().get(header::ACCEPT) {
        Some(entry) if entry == HeaderValue::from_static(CONTENT_TYPE) => {
            HttpResponse::Ok().content_type(CONTENT_TYPE).body(
                req.state()
                    .json
                    .read()
                    .expect("json lock has been poisoned")
                    .clone(),
            )
        }
        _ => HttpResponse::NotAcceptable().finish(),
    }
}

#[derive(Clone)]
pub struct State {
    pub(crate) json: Arc<RwLock<String>>,
}

impl State {
    pub fn new() -> State {
        State {
            json: Arc::new(RwLock::new(String::new())),
        }
    }
}

pub(crate) fn create_graph(
    opts: &config::Options,
    releases: Vec<registry::Release>,
) -> Result<Graph, Error> {
    let mut graph = Graph::default();

    if releases.is_empty() {
        warn!(
            "could not find any releases in {}/{}",
            &opts.registry, &opts.repository
        );
        return Ok(graph);
    };

    releases
        .into_iter()
        .inspect(|release| trace!("Adding a release to the graph '{:?}'", release))
        .try_for_each(|release| {
            let previous = release.metadata.previous.clone();
            let next = release.metadata.next.clone();
            let current = graph.add_release(release)?;

            previous.iter().try_for_each(|version| {
                let previous = match graph.find_by_version(&version.to_string()) {
                    Some(id) => id,
                    None => graph.add_release(Release::Abstract(AbstractRelease {
                        version: version.to_string(),
                    }))?,
                };
                graph.add_transition(&previous, &current)
            })?;

            next.iter().try_for_each(|version| {
                let next = match graph.find_by_version(&version.to_string()) {
                    Some(id) => id,
                    None => graph.add_release(Release::Abstract(AbstractRelease {
                        version: version.to_string(),
                    }))?,
                };
                graph.add_transition(&current, &next)
            })
        })?;

    Ok(graph)
}
