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

use crate::common::async_util::BoxFuture;
use dashmap::DashMap;
use http::{Request, Response};
use std::collections::HashSet;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::Body as TonicBody;
use tower::{
    BoxError, Service, balance::p2c::Balance, buffer::Buffer, discover::Discover, load::Load,
};

type RespFut<Resp> = BoxFuture<Result<Resp, BoxError>>;

const DEFAULT_BUFFER_CAPACITY: usize = 1024;

/// `ClusterBalancer` is responsible for managing load balancing requests across multiple channels.
/// Currently, `ClusterBalancer` leverges `tower::balance::p2c` for doing P2C load balancing. In the future, we will
/// support more load balancing strategies as needed.
pub(crate) struct ClusterBalancer<D, Req>
where
    D: Discover,
    D::Key: Hash,
{
    balancer: Balance<D, Req>,
}

impl<D, Req> ClusterBalancer<D, Req>
where
    D: Discover,
    D::Key: Hash,
    D::Service: Service<Req>,
    <D::Service as Service<Req>>::Error: Into<BoxError>,
{
    /// Creates a new `ClusterBalancer` with provided service discovery.
    pub(crate) fn new(discover: D) -> Self {
        Self {
            balancer: Balance::new(discover),
        }
    }

    /// Returns the number of endpoints currently tracked by the balancer.
    /// This can be useful for monitoring and debugging purposes.
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.balancer.len()
    }
}

impl<D, Req> Service<Req> for ClusterBalancer<D, Req>
where
    D: Discover + Unpin,
    D::Key: Hash + Clone,
    D::Error: Into<BoxError>,
    D::Service: Service<Req> + Load,
    <D::Service as Load>::Metric: std::fmt::Debug,
    <D::Service as Service<Req>>::Error: Into<BoxError> + 'static,
    <D::Service as Service<Req>>::Future: Send + 'static,
{
    type Response = <Balance<D, Req> as Service<Req>>::Response;
    type Error = <Balance<D, Req> as Service<Req>>::Error;
    type Future = RespFut<Self::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.balancer.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        Box::pin(self.balancer.call(req))
    }
}

/// `ClusterChannel` is similar to `tonic::transport::Channel`, but is for load-balancing across all
/// the channels for a xDS Cluster.
/// `ClusterChannel` should be cloned to be used in multi-threaded environment. It leverages a `tower::Buffer` to
/// queue requests from multiple callers and behind the queue, it load-balances the requests across all
/// available channels by leveraging the inner `ClusterBalancer` object.
pub(crate) struct ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    // The mpsc channel between callers and the actual pool of channels.
    svc: Buffer<Req, BoxFuture<Result<Resp, BoxError>>>,
}

impl<Req, Resp> Clone for ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    fn clone(&self) -> Self {
        Self {
            svc: self.svc.clone(),
        }
    }
}

impl<Req, Resp> ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    /// Creates a new `ClusterChannel` with the given service and picker.
    pub(crate) fn from_balancer<B>(balancer: B, buffer_cap: usize) -> Self
    where
        B: Service<Req, Error = BoxError, Future = RespFut<Resp>> + Send + 'static,
    {
        let svc = Buffer::new(balancer, buffer_cap);
        Self { svc }
    }
}

impl<Req, Resp> Service<Req> for ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    type Response = Resp;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.svc, cx).map_err(BoxError::from)
    }

    fn call(&mut self, request: Req) -> Self::Future {
        Box::pin(self.svc.call(request))
    }
}

/// `ClusterClient` manages channels that load-balance for a xDS cluster.
pub(crate) struct ClusterClient<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    name: String,
    channel: ClusterChannel<Req, Resp>,
}

impl Debug for ClusterClient<(), ()> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterClient")
            .field("name", &self.name)
            .finish()
    }
}

impl<Req, Resp> ClusterClient<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    /// Creates a new `ClusterClient` with the given cluster name and service discovery implementation.
    /// Currently, `tower::discover::Discover` is used for service discovery.
    pub(crate) fn new<D>(name: String, discover: D) -> Self
    where
        D: Discover + Unpin + Send + 'static,
        D::Key: std::hash::Hash + Clone + Send,
        D::Error: Into<BoxError>,
        D::Service: Service<Req, Response = Resp> + Load + Send + 'static,
        <D::Service as Load>::Metric: std::fmt::Debug,
        <D::Service as Service<Req>>::Error: Into<BoxError>,
        <D::Service as Service<Req>>::Future: Send + 'static,
    {
        let balancer = ClusterBalancer::new(discover);
        let channel = ClusterChannel::from_balancer(balancer, DEFAULT_BUFFER_CAPACITY);
        Self { name, channel }
    }

    /// Returns a channel that can be used to send RPCs to the cluster.
    pub(crate) fn channel(&self) -> ClusterChannel<Req, Resp> {
        self.channel.clone()
    }

    /// Returns the name of the cluster.
    #[allow(dead_code)]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

/// `ClusterRegistry` is the client registry for all xDS clusters.
/// The xDS Tower service implementations uses this to get the client for a specific cluster.
pub(crate) struct ClusterClientRegistry<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    registry: DashMap<String, Arc<ClusterClient<Req, Resp>>>,
}

impl<Req, Resp> ClusterClientRegistry<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    /// Creates a new `ClusterClientRegistry`.
    pub(crate) fn new() -> Self {
        Self {
            registry: DashMap::new(),
        }
    }

    /// Looks up the client for a cluster.
    ///
    /// Returns `None` for clusters outside the set from the last
    /// [`sync_clusters`](Self::sync_clusters) call — e.g. a request carrying
    /// a route decision for a cluster that has since left the route config.
    /// Callers must fail the request; only `sync_clusters` creates clients.
    pub(crate) fn get_cluster(&self, key: &str) -> Option<Arc<ClusterClient<Req, Resp>>> {
        self.registry
            .get(key)
            .map(|entry| Arc::clone(entry.value()))
    }

    /// Reconciles the registry against `active`, the authoritative cluster
    /// set from the current route configuration: drops every client outside
    /// it and creates one (via `discover_fn`) for each newly added cluster.
    ///
    /// This is the only way clients enter the registry — the request path
    /// ([`get_cluster`](Self::get_cluster)) can only look them up. Creating
    /// clients on the request path would let a request racing an eviction
    /// (e.g. a retry carrying a stale route decision) re-insert a
    /// just-removed cluster as a permanently-unready client that hangs
    /// requests and leaks for the channel's lifetime. In-flight requests
    /// keep their clones of an evicted client alive until they complete.
    pub(crate) fn sync_clusters<F, D>(&self, active: &HashSet<String>, mut discover_fn: F)
    where
        F: FnMut(&str) -> D,
        D: Discover + Unpin + Send + 'static,
        D::Key: std::hash::Hash + Clone + Send,
        D::Error: Into<BoxError>,
        D::Service: Service<Req, Response = Resp> + Load + Send + 'static,
        <D::Service as Load>::Metric: std::fmt::Debug,
        <D::Service as Service<Req>>::Error: Into<BoxError>,
        <D::Service as Service<Req>>::Future: Send + 'static,
    {
        self.registry.retain(|name, _| active.contains(name));
        for name in active {
            self.registry
                .entry(name.clone())
                .or_insert_with(|| Arc::new(ClusterClient::new(name.clone(), discover_fn(name))));
        }
    }
}

impl<Req, Resp> Default for ClusterClientRegistry<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A type erased registry for Tonic clients.
/// This will be used by the xDS Tower Service implementations to get the client for a specific Tonic xDS cluster.
#[allow(dead_code)]
pub(crate) type ClusterClientRegistryGrpc =
    ClusterClientRegistry<Request<TonicBody>, Response<TonicBody>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::endpoint::{EndpointAddress, EndpointChannel};
    use crate::client::lb::BoxDiscover;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Channel;

    fn empty_discover(_name: &str) -> BoxDiscover<EndpointAddress, EndpointChannel<Channel>> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Box::pin(ReceiverStream::new(rx))
    }

    fn set(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn get_cluster_only_finds_synced_clusters() {
        let registry = ClusterClientRegistryGrpc::new();
        assert!(registry.get_cluster("a").is_none());

        registry.sync_clusters(&set(&["a"]), empty_discover);
        assert!(registry.get_cluster("a").is_some());
        assert!(registry.get_cluster("b").is_none());
    }

    /// A request racing an eviction (stale route decision) gets a lookup
    /// miss — it cannot resurrect the evicted client — and a cluster
    /// re-entering the config gets a fresh client rather than the old one.
    #[tokio::test]
    async fn sync_clusters_evicts_and_recreates() {
        let registry = ClusterClientRegistryGrpc::new();
        registry.sync_clusters(&set(&["a"]), empty_discover);
        let first = registry.get_cluster("a").unwrap();

        registry.sync_clusters(&set(&[]), empty_discover);
        assert!(registry.get_cluster("a").is_none());

        registry.sync_clusters(&set(&["a"]), empty_discover);
        let second = registry.get_cluster("a").unwrap();
        assert!(
            !Arc::ptr_eq(&first, &second),
            "a re-added cluster must get a fresh client, not the evicted one",
        );
    }

    /// Re-syncing with an unchanged set keeps the existing clients (and
    /// their warm connections) instead of rebuilding them.
    #[tokio::test]
    async fn sync_clusters_keeps_existing_clients() {
        let registry = ClusterClientRegistryGrpc::new();
        registry.sync_clusters(&set(&["a"]), empty_discover);
        let first = registry.get_cluster("a").unwrap();

        registry.sync_clusters(&set(&["a", "b"]), empty_discover);
        let same = registry.get_cluster("a").unwrap();
        assert!(Arc::ptr_eq(&first, &same));
    }
}
