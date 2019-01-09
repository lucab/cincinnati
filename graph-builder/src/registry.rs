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

use cincinnati;
use failure::{Error, Fallible, ResultExt};
use flate2::read::GzDecoder;
use futures::future;
use futures::prelude::*;
use release::Metadata;
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::string::String;
use tar::Archive;
use tokio_core::reactor::Core;

#[derive(Debug, Clone, PartialEq)]
pub struct Release {
    pub source: String,
    pub metadata: Metadata,
}

impl Into<cincinnati::Release> for Release {
    fn into(self) -> cincinnati::Release {
        cincinnati::Release::Concrete(cincinnati::ConcreteRelease {
            version: self.metadata.version.to_string(),
            payload: self.source,
            metadata: self.metadata.metadata,
        })
    }
}

fn trim_protocol(src: &str) -> &str {
    src.trim_left_matches("https://")
        .trim_left_matches("http://")
}

pub fn read_credentials(
    credentials_path: Option<&PathBuf>,
    registry: &str,
) -> Result<(Option<String>, Option<String>), Error> {
    credentials_path.clone().map_or(Ok((None, None)), |path| {
        Ok(
            dkregistry::get_credentials(File::open(&path)?, trim_protocol(registry))
                .map_err(|e| format_err!("{}", e))?,
        )
    })
}

fn authenticate_client(
    client: dkregistry::v2::Client,
    login_scope: String,
) -> impl Future<Item = dkregistry::v2::Client, Error = Error> {
    client
        .is_v2_supported()
        .and_then(move |v2_supported| {
            if !v2_supported {
                Err("API v2 not supported".into())
            } else {
                Ok(client)
            }
        })
        .and_then(move |mut dclient| {
            dclient.login(&[&login_scope]).and_then(move |token| {
                dclient
                    .is_auth(Some(token.token()))
                    .and_then(move |is_auth| {
                        if !is_auth {
                            Err("login failed".into())
                        } else {
                            dclient.set_token(Some(token.token()));
                            Ok(dclient)
                        }
                    })
            })
        })
        .map_err(|e| format_err!("{}", e))
}

/// Fetches a vector of all release metadata from the given repository, hosted on the given
/// registry.
pub fn fetch_releases(
    registry: &str,
    repo: &str,
    username: Option<&str>,
    password: Option<&str>,
    cache: &mut HashMap<u64, Option<Release>>,
) -> Result<Vec<Release>, Error> {
    let registry_host = trim_protocol(&registry);
    let login_scope = format!("repository:{}:pull", &repo);

    let client = dkregistry::v2::Client::configure(&Core::new()?.handle())
        .registry(registry_host)
        .insecure_registry(false)
        .username(username.map(|s| s.to_string()))
        .password(password.map(|s| s.to_string()))
        .build()
        .map_err(|e| format_err!("{}", e));

    let tagged_layers = {
        let mut thread_runtime = tokio::runtime::current_thread::Runtime::new()?;
        let fetch_tags = future::result(client)
            .map(move |client| (client, login_scope))
            .and_then(|(client, scope)| authenticate_client(client, scope))
            .and_then(|authenticated_client| {
                let tags_stream = get_tags(repo, authenticated_client);
                future::ok(tags_stream)
            })
            .flatten_stream()
            .and_then(|(authenticated_client, tag)| {
                get_manifest_and_layers(tag, repo, authenticated_client)
            })
            .collect();
        thread_runtime.block_on(fetch_tags)?
    };

    let mut releases = Vec::with_capacity(tagged_layers.len());
    for (authenticated_client, tag, layer_digests) in tagged_layers {
        let release = cache_release(
            layer_digests,
            authenticated_client.to_owned(),
            registry_host.to_owned(),
            repo.to_owned(),
            tag.to_owned(),
            cache,
        )?;
        if let Some(metadata) = release {
            releases.push(metadata);
        };
    }
    releases.shrink_to_fit();

    Ok(releases)
}

/// Look up release metadata for a specific tag, and cache it.
///
/// Each tagged release is looked up at most once and both
/// positive (Some metadata) and negative (None) results cached
/// indefinitely.
///
/// Update Images with release metadata should be immutable, but
/// tags on registry can be mutated at any time. Thus, the cache
/// is keyed on the hash of tag layers.
fn cache_release(
    layer_digests: Vec<String>,
    authenticated_client: dkregistry::v2::Client,
    registry_host: String,
    repo: String,
    tag: String,
    cache: &mut HashMap<u64, Option<Release>>,
) -> Fallible<Option<Release>> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // TODO(lucab): get rid of this synchronous lookup, by
    // introducing a dedicated actor which owns the cache
    // and handles queries and insertions.
    let mut thread_runtime = tokio::runtime::current_thread::Runtime::new()?;

    let hashed_tag_layers = {
        let mut hasher = DefaultHasher::new();
        layer_digests.hash(&mut hasher);
        hasher.finish()
    };

    if let Some(release) = cache.get(&hashed_tag_layers) {
        trace!("Using cached release metadata for tag {}", &tag);
        return Ok(release.clone());
    }

    let tagged_release = find_first_release(
        layer_digests,
        authenticated_client,
        registry_host,
        repo,
        tag,
    );
    let (tag, release) = thread_runtime
        .block_on(tagged_release)
        .context("failed to find first release")?;

    trace!("Caching release metadata for new tag {}", &tag);
    cache.insert(hashed_tag_layers, release.clone());
    Ok(release)
}

/// Fetch all tags for a repository, as a stream.
///
/// Tags order depends on registry implementation.
/// According to [specs](https://docs.docker.com/registry/spec/api/#listing-image-tags),
/// remote API should return tags in lexicographic order.
/// However on Quay 2.9 this is not true.
fn get_tags(
    repo: &str,
    authenticated_client: dkregistry::v2::Client,
) -> impl Stream<Item = (dkregistry::v2::Client, String), Error = Error> {
    // Paginate results, 20 tags per page.
    let tags_per_page = Some(20);

    trace!("fetching tags for repo {}", repo);
    authenticated_client
        .get_tags(repo, tags_per_page)
        .map(move |tags| (authenticated_client.clone(), tags))
        .map_err(|e| format_err!("{}", e))
}

/// Fetch manifest for a tag, and return its layers digests.
fn get_manifest_and_layers(
    tag: String,
    repo: &str,
    authenticated_client: dkregistry::v2::Client,
) -> impl Future<Item = (dkregistry::v2::Client, String, Vec<String>), Error = failure::Error> {
    trace!("processing: {}:{}", repo, &tag);
    authenticated_client
        .has_manifest(repo, &tag, None)
        .join(authenticated_client.get_manifest(repo, &tag))
        .map_err(|e| format_err!("{}", e))
        .and_then(move |(manifest_kind, manifest)| get_layer_digests(&manifest_kind, &manifest))
        .map(move |digests| (authenticated_client, tag, digests))
}

fn find_first_release(
    layer_digests: Vec<String>,
    authenticated_client: dkregistry::v2::Client,
    registry_host: String,
    repo: String,
    repo_tag: String,
) -> impl Future<Item = (String, Option<Release>), Error = Error> {
    let tag = repo_tag.clone();

    let releases = layer_digests.into_iter().map(move |layer_digest| {
        trace!("Downloading layer {}...", &layer_digest);
        let (registry_host, repo, tag) = (registry_host.clone(), repo.clone(), repo_tag.clone());

        authenticated_client
            .get_blob(&repo, &layer_digest)
            .map_err(|e| format_err!("{}", e))
            .into_stream()
            .filter_map(move |blob| {
                let metadata_filename = "release-manifests/release-metadata";

                trace!(
                    "{}: Looking for {} in archive {} with {} bytes",
                    &tag,
                    &metadata_filename,
                    &layer_digest,
                    &blob.len(),
                );

                match assemble_metadata(&blob, metadata_filename) {
                    Ok(metadata) => Some(Release {
                        source: format!("{}/{}:{}", registry_host, repo, &tag),
                        metadata,
                    }),
                    Err(e) => {
                        debug!(
                            "could not assemble metadata from layer ({}) of tag '{}': {}",
                            &layer_digest, &tag, e,
                        );
                        None
                    }
                }
            })
    });

    futures::stream::iter_ok::<_, Error>(releases)
        .flatten()
        .take(1)
        .collect()
        .map(move |mut releases| {
            if releases.is_empty() {
                warn!("could not find any release in tag {}", tag);
                (tag, None)
            } else {
                (tag, Some(releases.remove(0)))
            }
        })
}

fn get_layer_digests(
    manifest_kind: &Option<dkregistry::mediatypes::MediaTypes>,
    manifest: &[u8],
) -> Result<Vec<String>, failure::Error> {
    use dkregistry::mediatypes::MediaTypes::{ManifestV2S1Signed, ManifestV2S2};
    use dkregistry::v2::manifest::{ManifestSchema1Signed, ManifestSchema2};

    match manifest_kind {
        Some(ManifestV2S1Signed) => serde_json::from_slice::<ManifestSchema1Signed>(manifest)
            .and_then(|m| {
                let mut l = m.get_layers();
                l.reverse();
                Ok(l)
            }),
        Some(ManifestV2S2) => serde_json::from_slice::<ManifestSchema2>(manifest).and_then(|m| {
            let mut l = m.get_layers();
            l.reverse();
            Ok(l)
        }),
        _ => bail!("unknown manifest_kind '{:?}'", manifest_kind),
    }
    .map_err(Into::into)
}

#[derive(Debug, Deserialize)]
struct Tags {
    name: String,
    tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    #[serde(rename = "schemaVersion")]
    schema_version: usize,
    name: String,
    tag: String,
    architecture: String,
    #[serde(rename = "fsLayers")]
    fs_layers: Vec<Layer>,
}

#[derive(Debug, Deserialize)]
struct Layer {
    #[serde(rename = "blobSum")]
    blob_sum: String,
}

fn assemble_metadata(blob: &[u8], metadata_filename: &str) -> Result<Metadata, Error> {
    let mut archive = Archive::new(GzDecoder::new(blob));
    match archive
        .entries()?
        .filter_map(|entry| match entry {
            Ok(file) => Some(file),
            Err(err) => {
                debug!("failed to read archive entry: {}", err);
                None
            }
        })
        .find(|file| match file.header().path() {
            Ok(path) => path == Path::new(metadata_filename),
            Err(err) => {
                debug!("failed to read file header: {}", err);
                false
            }
        }) {
        Some(mut file) => {
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            match serde_json::from_str::<Metadata>(&contents) {
                Ok(m) => Ok::<Metadata, Error>(m),
                Err(e) => bail!(format!("couldn't parse '{}': {}", metadata_filename, e)),
            }
        }
        None => bail!(format!("'{}' not found", metadata_filename)),
    }
    .map_err(Into::into)
}
