extern crate futures;
extern crate tokio;

use self::futures::prelude::*;
use self::tokio::runtime::Runtime;

fn common_init() -> (Runtime, Option<String>) {
    let _ = env_logger::try_init_from_env(env_logger::Env::default());
    let runtime = Runtime::new().unwrap();
    let token = std::env::var("CINCINNATI_TEST_QUAY_API_TOKEN")
        .expect("CINCINNATI_TEST_QUAY_API_TOKEN missing");
    (runtime, Some(token))
}

#[test]
fn test_wrong_auth() {
    let (mut rt, _) = common_init();
    let repo = "redhat/openshift-cincinnati-test-private-manual";

    let client = quay::v1::Client::builder()
        .access_token(Some("CLEARLY_WRONG".to_string()))
        .build()
        .unwrap();
    let fetch_tags = client.stream_tags(repo, true).collect();
    rt.block_on(fetch_tags).unwrap_err();
}

#[test]
fn test_stream_active_tags() {
    let (mut rt, token) = common_init();
    let repo = "redhat/openshift-cincinnati-test-private-manual";
    let expected = vec!["0.0.1", "0.0.0"];

    let client = quay::v1::Client::builder()
        .access_token(token)
        .build()
        .unwrap();
    let fetch_tags = client.stream_tags(repo, true).collect();
    let tags = rt.block_on(fetch_tags).unwrap();

    let tag_names: Vec<String> = tags.into_iter().map(|tag| tag.name).collect();
    assert_eq!(tag_names, expected);
}

#[test]
fn test_get_labels() {
    let (mut rt, token) = common_init();
    let repo = "redhat/openshift-cincinnati-test-private-manual";
    let tag_name = "0.0.0";

    let client = quay::v1::Client::builder()
        .access_token(token)
        .build()
        .unwrap();
    let fetch_tags = client.stream_tags(repo, true).collect();
    let tags = rt.block_on(fetch_tags).unwrap();

    let filtered_tags: Vec<quay::v1::Tag> = tags
        .into_iter()
        .filter(|tag| tag.name == tag_name)
        .collect();
    assert_eq!(filtered_tags.len(), 1);

    let tag = &filtered_tags[0];
    assert_eq!(tag.name, tag_name);

    let digest = tag.manifest_digest.clone().unwrap();
    let fetch_labels = client.get_labels(repo.to_string(), digest);
    let labels = rt.block_on(fetch_labels).unwrap();
    assert_eq!(labels, vec![]);
}