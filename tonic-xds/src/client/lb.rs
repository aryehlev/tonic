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

use crate::client::cluster::ClusterClientRegistry;
use crate::client::route::RouteDecision;
use crate::common::async_util::BoxFuture;
use http::Request;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::ServiceExt;
use tower::{BoxError, Service, discover::Change};

/// A pinned, boxed stream of endpoint changes for Tower's `Discover`-based
/// load balancers.
pub(crate) type BoxDiscover<Endpoint, S> =
    Pin<Box<dyn futures_core::Stream<Item = Result<Change<Endpoint, S>, BoxError>> + Send>>;

/// Trait for discovering cluster endpoints.
///
/// Implementations resolve a cluster name into a stream of endpoint changes
/// (`Change::Insert` / `Change::Remove`). The xDS-backed implementation is
/// [`XdsClusterDiscovery`](crate::xds::cluster_discovery::XdsClusterDiscovery).
pub(crate) trait ClusterDiscovery<Endpoint, S>: Send + Sync + 'static {
    fn discover_cluster(&self, cluster_name: &str) -> BoxDiscover<Endpoint, S>;
}

/// Errors that can occur during load balancing.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum LoadBalancingError {
    #[error("No routing decision extension from the routing layer available")]
    NoRoutingDecision,
    #[error("no client for cluster '{0}': not in the current route configuration")]
    ClusterNotFound(String),
}

/// A Tower Service that forwards each request to the client of the cluster
/// chosen by the routing layer.
///
/// This is a pure lookup: cluster clients are created and destroyed only by
/// the route-config reconcile (via `ClusterClientRegistry::add_clusters` /
/// `evict_clusters`), never by the request path. A request whose decision
/// names a cluster with no client — e.g. one that left the route config
/// after the decision was made — fails fast with UNAVAILABLE instead of
/// waiting on a cluster that will never be ready.
pub(crate) struct XdsLbService<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    cluster_registry: Arc<ClusterClientRegistry<Req, Resp>>,
}

impl<Req, Resp> XdsLbService<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    /// Creates a new `XdsLbService` with the given cluster client registry.
    pub(crate) fn new(cluster_registry: Arc<ClusterClientRegistry<Req, Resp>>) -> Self {
        Self { cluster_registry }
    }
}

impl<Req, Resp> Clone for XdsLbService<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    fn clone(&self) -> Self {
        Self {
            cluster_registry: self.cluster_registry.clone(),
        }
    }
}

impl<B, Resp> Service<Request<B>> for XdsLbService<Request<B>, Resp>
where
    Request<B>: Send + 'static,
    Resp: Send + 'static,
{
    type Response = Resp;
    type Error = BoxError;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Under xDS, the destination cluster is decided by the routing layer, which takes
        // the request as an input. Therefore, we cannot determine readiness without
        // knowing the target cluster, which is tied to the request.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        // Extract the routing decision from the request extensions.
        let Some(routing_decision) = request.extensions().get::<RouteDecision>().cloned() else {
            return Box::pin(async move { Err(LoadBalancingError::NoRoutingDecision.into()) });
        };

        let Some(cluster_client) = self.cluster_registry.get_cluster(&routing_decision.cluster)
        else {
            // A `tonic::Status` (not the bare enum, which tonic would map to
            // Unknown): the miss is a transient race with a config update
            // removing the cluster, and UNAVAILABLE is the code callers and
            // retry policies key on for such conditions.
            return Box::pin(async move {
                Err(tonic::Status::unavailable(
                    LoadBalancingError::ClusterNotFound(routing_decision.cluster).to_string(),
                )
                .into())
            });
        };

        // Get the transport channel for the target xDS cluster.
        // The actual load-balancing will be performed by the cluster's balancer.
        let mut channel = cluster_client.channel();

        Box::pin(async move {
            // This will block until the first endpoint is available.
            channel.ready().await?;
            channel.call(request).await
        })
    }
}
