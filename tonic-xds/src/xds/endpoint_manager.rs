/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

//! Converts snapshot-based endpoint cache updates into incremental
//! [`Change`] streams for Tower's load balancing infrastructure.
//!
//! The resource manager writes [`EndpointsResource`] snapshots into the
//! `XdsCache`; this module diffs consecutive snapshots and produces
//! `Change::Insert` / `Change::Remove` events that Tower's P2C balancer
//! (or any other `Discover`-based balancer) can consume.

use std::collections::HashSet;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;

use arc_swap::ArcSwap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tower::BoxError;
use tower::discover::Change;

use crate::client::endpoint::{
    CONNECT_BACKOFF_INITIAL, CONNECT_BACKOFF_MAX, Connector, EndpointAddress,
};
use crate::client::lb::BoxDiscover;
use crate::client::loadbalance::keyed_futures::KeyedFutures;
use crate::xds::cache::CacheWatch;
use crate::xds::resource::EndpointsResource;

/// Buffer capacity for the endpoint change channel between the diff loop
/// and Tower's load balancer.
const ENDPOINT_CHANNEL_CAPACITY: usize = 64;

/// An atomically-swappable [`Connector`] held by an [`EndpointManager`].
///
/// `XdsClusterDiscovery` stores a snapshot of the cluster's per-CDS-update
/// connector here. The diff loop calls `load_full()` on every new endpoint
/// so each connection picks up the latest snapshot. Existing endpoint
/// channels keep their `EndpointChannel` instance (and any in-flight TLS
/// session) — only freshly-discovered endpoints see the swapped value.
pub(crate) type ConnectorSwap<S> = Arc<ArcSwap<Arc<dyn Connector<Service = S> + Send + Sync>>>;

/// Converts endpoint cache watches into incremental [`Change`] streams.
///
/// `EndpointManager` is a pure diff-and-connect component: the caller
/// (typically `XdsClusterDiscovery`) obtains a [`CacheWatch`] from the
/// [`XdsCache`](crate::xds::cache::XdsCache) and passes it here, plus a
/// [`ConnectorSwap`] that the caller may swap on CDS updates.
pub(crate) struct EndpointManager<S: Send + 'static> {
    connector: ConnectorSwap<S>,
}

impl<S: Send + 'static> EndpointManager<S> {
    pub(crate) fn new(connector: ConnectorSwap<S>) -> Self {
        Self { connector }
    }

    /// Returns a stream of endpoint changes for the cache watch produced by
    /// `watch_factory`.
    ///
    /// Diffs each snapshot against the previous set of healthy endpoints,
    /// emitting `Change::Insert` for connected new endpoints and
    /// `Change::Remove` for removed ones. When the watched cache entry is
    /// removed the stream drains and re-subscribes via `watch_factory`
    /// instead of ending: `ClusterClientRegistry` caches clients by cluster
    /// name forever, so a name that leaves the config and later returns
    /// (e.g. a canary cluster on the next rollout) must keep resolving.
    pub(crate) fn discover_endpoints<W>(&self, watch_factory: W) -> BoxDiscover<EndpointAddress, S>
    where
        W: Fn() -> CacheWatch<EndpointsResource> + Send + 'static,
    {
        let connector = self.connector.clone();
        let (tx, rx) = mpsc::channel(ENDPOINT_CHANNEL_CAPACITY);

        // The spawned task exits only when the receiver is dropped
        // (consumer no longer reading Change events).
        tokio::spawn(diff_loop(watch_factory, connector, tx));

        Box::pin(ReceiverStream::new(rx))
    }
}

/// Background task: watches endpoint snapshots and emits incremental changes.
///
/// Each time a new [`EndpointsResource`] arrives from the cache, we diff
/// `healthy_endpoints()` against the desired set. Added endpoints start a
/// concurrent connection attempt (retrying with backoff until the address is
/// removed); `Change::Insert` is emitted only once the connection is actually
/// established, so the balancer never routes requests to an unconnected
/// endpoint. Removed endpoints cancel any in-flight attempt and emit
/// `Change::Remove` if they had been inserted.
async fn diff_loop<S: Send + 'static, W>(
    watch_factory: W,
    connector: ConnectorSwap<S>,
    tx: mpsc::Sender<Result<Change<EndpointAddress, S>, BoxError>>,
) where
    W: Fn() -> CacheWatch<EndpointsResource> + Send + 'static,
{
    let mut watch = watch_factory();
    // Endpoints in the latest snapshot.
    let mut desired: HashSet<EndpointAddress> = HashSet::new();
    // Endpoints whose `Insert` has been sent to the balancer.
    let mut inserted: HashSet<EndpointAddress> = HashSet::new();
    // In-flight connection attempts, cancellable by address.
    let mut connecting: KeyedFutures<EndpointAddress, S> = KeyedFutures::new();

    loop {
        tokio::select! {
            snapshot = watch.next() => {
                let Some(endpoints) = snapshot else {
                    // Cache entry removed: the cluster left the config.
                    // Drain the balancer and re-subscribe — the same name
                    // may return (e.g. the next canary rollout), and this
                    // stream must outlive the removal because the cluster
                    // client registry caches clients by name.
                    for addr in std::mem::take(&mut desired) {
                        let _ = connecting.cancel(&addr);
                        if inserted.remove(&addr)
                            && tx.send(Ok(Change::Remove(addr))).await.is_err()
                        {
                            return;
                        }
                    }
                    watch = watch_factory();
                    continue;
                };
                let new_set: HashSet<EndpointAddress> = endpoints
                    .healthy_endpoints()
                    .map(|ep| ep.address.clone())
                    .collect();

                for added in new_set.difference(&desired) {
                    let _ = connecting.add(
                        added.clone(),
                        connect_with_backoff(connector.clone(), added.clone()),
                    );
                }
                for removed in desired.difference(&new_set) {
                    let _ = connecting.cancel(removed);
                    if inserted.remove(removed)
                        && tx.send(Ok(Change::Remove(removed.clone()))).await.is_err()
                    {
                        return;
                    }
                }
                desired = new_set;
            }
            (addr, svc) = poll_fn(|cx| match connecting.poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(item),
                // `None` means no attempts in flight; wait for snapshots.
                Poll::Ready(None) | Poll::Pending => Poll::Pending,
            }) => {
                if desired.contains(&addr) {
                    inserted.insert(addr.clone());
                    if tx.send(Ok(Change::Insert(addr, svc))).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// Connects to `addr`, retrying with exponential backoff until it succeeds.
///
/// The connector is re-read from the swap on every attempt so CDS updates
/// (e.g. rotated TLS config) apply to retries. Resolves only on success;
/// abandoning the future (via [`KeyedFutures::cancel`]) aborts the attempt.
async fn connect_with_backoff<S: Send + 'static>(
    connector: ConnectorSwap<S>,
    addr: EndpointAddress,
) -> S {
    let mut backoff = CONNECT_BACKOFF_INITIAL;
    loop {
        match connector.load_full().connect(&addr).await {
            Ok(svc) => return svc,
            Err(error) => {
                tracing::warn!(
                    address = %addr,
                    error = %error,
                    backoff_secs = backoff.as_secs(),
                    "endpoint connect failed; retrying",
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(CONNECT_BACKOFF_MAX);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::async_util::BoxFuture;
    use crate::xds::cache::XdsCache;
    use crate::xds::resource::endpoints::{HealthStatus, LocalityEndpoints, ResolvedEndpoint};
    use tokio_stream::StreamExt;

    /// Test [`Connector`] that returns the address as its `Service` (just a
    /// `String`).
    struct StringConnector;

    impl Connector for StringConnector {
        type Service = String;
        fn connect(&self, addr: &EndpointAddress) -> BoxFuture<Result<Self::Service, BoxError>> {
            let s = addr.to_string();
            Box::pin(async move { Ok(s) })
        }
    }

    fn test_swap() -> ConnectorSwap<String> {
        let conn: Arc<dyn Connector<Service = String> + Send + Sync> = Arc::new(StringConnector);
        Arc::new(ArcSwap::from_pointee(conn))
    }

    fn make_endpoints(cluster: &str, addrs: &[(&str, u16)]) -> Arc<EndpointsResource> {
        Arc::new(EndpointsResource {
            cluster_name: cluster.to_string(),
            localities: vec![LocalityEndpoints {
                locality: None,
                endpoints: addrs
                    .iter()
                    .map(|(host, port)| ResolvedEndpoint {
                        address: EndpointAddress::new(*host, *port),
                        health_status: HealthStatus::Healthy,
                        load_balancing_weight: 1,
                    })
                    .collect(),
                load_balancing_weight: 100,
                priority: 0,
            }],
        })
    }

    #[tokio::test]
    async fn initial_endpoints_emitted_as_inserts() {
        let cache = Arc::new(XdsCache::new());
        let manager = EndpointManager::new(test_swap());

        cache.update_endpoints(
            "c1",
            make_endpoints("c1", &[("10.0.0.1", 8080), ("10.0.0.2", 8080)]),
        );

        let mut stream = manager.discover_endpoints({
            let cache = cache.clone();
            move || cache.watch_endpoints("c1")
        });

        let mut addrs: Vec<String> = Vec::new();
        for _ in 0..2 {
            match stream.next().await.unwrap().unwrap() {
                Change::Insert(addr, _svc) => addrs.push(addr.to_string()),
                Change::Remove(_) => panic!("expected Insert"),
            }
        }
        addrs.sort();
        assert_eq!(addrs, vec!["10.0.0.1:8080", "10.0.0.2:8080"]);
    }

    #[tokio::test]
    async fn added_endpoint_emits_insert() {
        let cache = Arc::new(XdsCache::new());
        let manager = EndpointManager::new(test_swap());

        cache.update_endpoints("c1", make_endpoints("c1", &[("10.0.0.1", 8080)]));

        let mut stream = manager.discover_endpoints({
            let cache = cache.clone();
            move || cache.watch_endpoints("c1")
        });
        let _ = stream.next().await; // consume initial

        cache.update_endpoints(
            "c1",
            make_endpoints("c1", &[("10.0.0.1", 8080), ("10.0.0.2", 8080)]),
        );

        match stream.next().await.unwrap().unwrap() {
            Change::Insert(addr, _) => assert_eq!(addr.to_string(), "10.0.0.2:8080"),
            Change::Remove(_) => panic!("expected Insert for new endpoint"),
        }
    }

    #[tokio::test]
    async fn removed_endpoint_emits_remove() {
        let cache = Arc::new(XdsCache::new());
        let manager = EndpointManager::new(test_swap());

        cache.update_endpoints(
            "c1",
            make_endpoints("c1", &[("10.0.0.1", 8080), ("10.0.0.2", 8080)]),
        );

        let mut stream = manager.discover_endpoints({
            let cache = cache.clone();
            move || cache.watch_endpoints("c1")
        });
        // Consume 2 initial inserts.
        let _ = stream.next().await;
        let _ = stream.next().await;

        // Shrink to one endpoint.
        cache.update_endpoints("c1", make_endpoints("c1", &[("10.0.0.1", 8080)]));

        match stream.next().await.unwrap().unwrap() {
            Change::Remove(addr) => assert_eq!(addr.to_string(), "10.0.0.2:8080"),
            Change::Insert(..) => panic!("expected Remove"),
        }
    }

    #[tokio::test]
    async fn unhealthy_endpoint_removed() {
        let cache = Arc::new(XdsCache::new());
        let manager = EndpointManager::new(test_swap());

        cache.update_endpoints("c1", make_endpoints("c1", &[("10.0.0.1", 8080)]));

        let mut stream = manager.discover_endpoints({
            let cache = cache.clone();
            move || cache.watch_endpoints("c1")
        });
        let _ = stream.next().await; // consume initial insert

        let unhealthy = Arc::new(EndpointsResource {
            cluster_name: "c1".to_string(),
            localities: vec![LocalityEndpoints {
                locality: None,
                endpoints: vec![ResolvedEndpoint {
                    address: EndpointAddress::new("10.0.0.1", 8080),
                    health_status: HealthStatus::Unhealthy,
                    load_balancing_weight: 1,
                }],
                load_balancing_weight: 100,
                priority: 0,
            }],
        });
        cache.update_endpoints("c1", unhealthy);

        match stream.next().await.unwrap().unwrap() {
            Change::Remove(addr) => assert_eq!(addr.to_string(), "10.0.0.1:8080"),
            Change::Insert(..) => panic!("expected Remove for unhealthy endpoint"),
        }
    }

    #[tokio::test]
    async fn cache_removal_drains_and_survives_readdition() {
        let cache = Arc::new(XdsCache::new());
        let manager = EndpointManager::new(test_swap());

        cache.update_endpoints("c1", make_endpoints("c1", &[("10.0.0.1", 8080)]));

        let mut stream = manager.discover_endpoints({
            let cache = cache.clone();
            move || cache.watch_endpoints("c1")
        });
        let _ = stream.next().await; // consume initial Insert

        // Removal drains the balancer but does not end the stream: the
        // cluster name may return (registry clients live forever).
        cache.remove_endpoints("c1");
        match stream.next().await.unwrap().unwrap() {
            Change::Remove(addr) => assert_eq!(addr.to_string(), "10.0.0.1:8080"),
            Change::Insert(..) => panic!("expected Remove"),
        }

        // Re-addition under the same name resolves fresh endpoints.
        cache.update_endpoints("c1", make_endpoints("c1", &[("10.0.0.2", 9090)]));
        match stream.next().await.unwrap().unwrap() {
            Change::Insert(addr, _) => assert_eq!(addr.to_string(), "10.0.0.2:9090"),
            Change::Remove(_) => panic!("expected Insert"),
        }
    }

    #[tokio::test]
    async fn multiple_clusters_independent() {
        let cache = Arc::new(XdsCache::new());
        let manager = EndpointManager::new(test_swap());

        cache.update_endpoints("c1", make_endpoints("c1", &[("10.0.0.1", 8080)]));
        cache.update_endpoints("c2", make_endpoints("c2", &[("10.0.0.2", 9090)]));

        let mut s1 = manager.discover_endpoints({
            let cache = cache.clone();
            move || cache.watch_endpoints("c1")
        });
        let mut s2 = manager.discover_endpoints({
            let cache = cache.clone();
            move || cache.watch_endpoints("c2")
        });

        match s1.next().await.unwrap().unwrap() {
            Change::Insert(addr, _) => assert_eq!(addr.to_string(), "10.0.0.1:8080"),
            _ => panic!("expected Insert"),
        }
        match s2.next().await.unwrap().unwrap() {
            Change::Insert(addr, _) => assert_eq!(addr.to_string(), "10.0.0.2:9090"),
            _ => panic!("expected Insert"),
        }
    }

    #[tokio::test]
    async fn endpoint_swap_emits_insert_then_remove() {
        let cache = Arc::new(XdsCache::new());
        let manager = EndpointManager::new(test_swap());

        cache.update_endpoints("c1", make_endpoints("c1", &[("10.0.0.1", 8080)]));

        let mut stream = manager.discover_endpoints({
            let cache = cache.clone();
            move || cache.watch_endpoints("c1")
        });
        let _ = stream.next().await; // consume initial

        cache.update_endpoints("c1", make_endpoints("c1", &[("10.0.0.2", 8080)]));

        let mut saw_remove = false;
        let mut saw_insert = false;
        for _ in 0..2 {
            match stream.next().await.unwrap().unwrap() {
                Change::Remove(addr) => {
                    assert_eq!(addr.to_string(), "10.0.0.1:8080");
                    saw_remove = true;
                }
                Change::Insert(addr, _) => {
                    assert_eq!(addr.to_string(), "10.0.0.2:8080");
                    saw_insert = true;
                }
            }
        }
        assert!(saw_remove, "should have removed old endpoint");
        assert!(saw_insert, "should have inserted new endpoint");
    }
}
