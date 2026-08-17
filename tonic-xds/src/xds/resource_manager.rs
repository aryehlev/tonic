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

//! xDS resource manager: LDS -> RDS -> CDS -> EDS cascade.
//!
//! The [`XdsResourceManager`] bridges the xDS client (ADS protocol layer) to the
//! [`XdsCache`]. It watches resources via
//! [`XdsClient::watch()`] and writes validated resources into the cache for
//! downstream consumers (routing layer, endpoint manager).
//!
//! # Cascade
//!
//! ```text
//! LDS -> RDS (or inline) -> CDS (per cluster) -> EDS (per cluster)
//! ```
//!
//! Each level determines the subscriptions for the next. When the set of
//! referenced clusters changes, the manager reconciles CDS/EDS watches:
//! adding watches for new clusters immediately, and dropping watches
//! (+ cache entries + per-cluster clients) for removed ones only after
//! [`CLUSTER_REMOVAL_GRACE`], so in-flight requests drain and transient
//! config flaps don't tear down warm clients.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Instant;

use futures_core::Stream;
use tokio_stream::{StreamExt, StreamMap};
use xds_client::{Resource, ResourceEvent, ResourceWatcher, XdsClient};

use crate::common::async_util::AbortOnDrop;
use crate::xds::cache::XdsCache;
use crate::xds::resource::listener::RouteSource;
use crate::xds::resource::{
    ClusterResource, EndpointsResource, ListenerResource, RouteConfigResource,
};

/// Adapter to use [`ResourceWatcher`] with [`StreamMap`].
struct WatcherStream<T: Resource>(ResourceWatcher<T>);

impl<T: Resource> Unpin for WatcherStream<T> {}

impl<T: Resource> Stream for WatcherStream<T> {
    type Item = ResourceEvent<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().0.poll_next(cx)
    }
}

/// Manages the LDS -> RDS -> CDS -> EDS cascade.
///
/// Subscribes to xDS resources via [`XdsClient::watch()`] and writes validated
/// resources into [`XdsCache`]. Dropping the manager aborts the background task.
pub(crate) struct XdsResourceManager {
    _task: AbortOnDrop,
}

impl XdsResourceManager {
    /// Creates a new resource manager and starts the cascade.
    ///
    /// # Arguments
    /// * `xds_client` - The xDS client for creating resource watches
    /// * `cache` - The shared cache to write resources into
    /// * `listener_name` - The LDS resource name to watch (from target URI)
    /// * `cluster_sync` - Keeps per-cluster clients matching the route
    ///   config on every reconcile (see [`ActiveClusterSync`])
    pub(crate) fn new(
        xds_client: XdsClient,
        cache: Arc<XdsCache>,
        listener_name: String,
        cluster_sync: Arc<dyn ActiveClusterSync>,
    ) -> Self {
        let state = CascadeState::new(cluster_sync);
        let handle = tokio::spawn(state.run(xds_client, cache, listener_name));
        Self {
            _task: AbortOnDrop(handle),
        }
    }
}

/// Keeps per-cluster clients in sync with the route config across a
/// reconcile. Implemented by
/// [`RegistryClusterSync`](crate::client::cluster::RegistryClusterSync)
/// over the `ClusterClientRegistry`.
///
/// The two halves run at different points of a reconcile:
/// [`add_clusters`](Self::add_clusters) before the new route config is
/// published, so a request routed on the new config always finds its
/// client; [`evict_clusters`](Self::evict_clusters) only once a removed
/// cluster's [`CLUSTER_REMOVAL_GRACE`] elapses, so requests routed on the
/// old config snapshot drain against a live client and a transient flap
/// never tears down a warm client.
pub(crate) trait ActiveClusterSync: Send + Sync {
    /// Create clients for clusters in `active` that don't have one yet.
    fn add_clusters(&self, active: &HashSet<String>);
    /// Drop the clients of clusters outside `keep`.
    fn evict_clusters(&self, keep: &HashSet<String>);
}

/// How long a cluster removed from the route config keeps its client,
/// watchers, and cached resources before teardown.
///
/// Two things ride on this delay: requests already routed on the previous
/// config snapshot (the router picks up a published config asynchronously)
/// still find a live client and drain instead of hard-failing, and a
/// transient control-plane flap that drops and restores a cluster within
/// the grace keeps the warm client — its established connections and
/// endpoint set — instead of rebuilding from scratch.
const CLUSTER_REMOVAL_GRACE: Duration = Duration::from_secs(5);

/// Mutable state for the entire LDS -> RDS -> CDS -> EDS cascade.
///
/// All four resource levels are polled in a single task via [`run`](Self::run).
struct CascadeState {
    /// Active RDS watcher — `None` if the listener uses inline routes.
    rds_watcher: Option<ResourceWatcher<RouteConfigResource>>,
    /// Active RDS name to detect changes across LDS updates.
    rds_name: Option<String>,
    /// Per-cluster CDS watchers, keyed by cluster name.
    cds_watchers: StreamMap<String, WatcherStream<ClusterResource>>,
    /// Per-cluster EDS watchers, keyed by cluster name.
    eds_watchers: StreamMap<String, WatcherStream<EndpointsResource>>,
    /// Current EDS service name per cluster, to detect when the name changes.
    eds_names: HashMap<String, String>,
    /// Keeps per-cluster clients matching the route config; driven by
    /// [`reconcile_and_publish`](Self::reconcile_and_publish) and
    /// [`process_due_removals`](Self::process_due_removals).
    cluster_sync: Arc<dyn ActiveClusterSync>,
    /// Clusters that left the route config, with the deadline at which
    /// their teardown becomes final. A cluster re-entering the config
    /// before its deadline is removed from here with nothing torn down.
    pending_removals: HashMap<String, Instant>,
}

impl CascadeState {
    fn new(cluster_sync: Arc<dyn ActiveClusterSync>) -> Self {
        Self {
            rds_watcher: None,
            rds_name: None,
            cds_watchers: StreamMap::new(),
            eds_watchers: StreamMap::new(),
            eds_names: HashMap::new(),
            cluster_sync,
            pending_removals: HashMap::new(),
        }
    }

    /// Runs the cascade select loop. All resource events are processed here.
    ///
    /// Biased so higher-level events (LDS/RDS) are processed before
    /// lower-level ones (CDS/EDS), avoiding wasted work on clusters
    /// about to be removed.
    async fn run(mut self, xds_client: XdsClient, cache: Arc<XdsCache>, listener_name: String) {
        let mut lds_watcher = xds_client.watch::<ListenerResource>(&listener_name).await;

        loop {
            let next_removal = self.pending_removals.values().min().copied();

            tokio::select! {
                biased;

                lds_event = lds_watcher.next() => {
                    // None means xds-client shut down; exit the cascade.
                    let Some(event) = lds_event else { break };
                    self.handle_lds(event, &xds_client, &cache).await;
                }

                rds_event = async {
                    match self.rds_watcher.as_mut() {
                        Some(w) => w.next().await,
                        // No active RDS watch (inline routes); disable this arm.
                        None => std::future::pending().await,
                    }
                } => {
                    // None means the RDS watcher closed; reset and wait for next LDS update.
                    let Some(event) = rds_event else {
                        self.rds_watcher = None;
                        self.rds_name = None;
                        continue;
                    };
                    self.handle_rds(event, &xds_client, &cache).await;
                }

                // A removed cluster's grace elapsed; tear it down — unless a
                // config update re-added it first (the LDS/RDS arms above
                // run first under `biased` and cancel the pending removal).
                _ = async {
                    match next_removal {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.process_due_removals(&cache);
                }

                Some((name, event)) = self.cds_watchers.next(),
                    if !self.cds_watchers.is_empty() =>
                {
                    self.handle_cds(&name, event, &xds_client, &cache).await;
                }

                Some((name, event)) = self.eds_watchers.next(),
                    if !self.eds_watchers.is_empty() =>
                {
                    self.handle_eds(&name, event, &cache);
                }
            }
        }
    }

    async fn handle_lds(
        &mut self,
        event: ResourceEvent<ListenerResource>,
        xds_client: &XdsClient,
        cache: &Arc<XdsCache>,
    ) {
        match event {
            ResourceEvent::ResourceChanged {
                result: Ok(listener),
                done,
            } => {
                match &listener.route_source {
                    RouteSource::Inline(rc) => {
                        // Drop any existing RDS watcher — routes are inline.
                        self.rds_watcher = None;
                        self.rds_name = None;

                        let rc = Arc::new(rc.clone());
                        self.reconcile_and_publish(rc, xds_client, cache).await;
                    }
                    RouteSource::Rds(rds_name) => {
                        if self.rds_name.as_deref() != Some(rds_name) {
                            self.rds_watcher =
                                Some(xds_client.watch::<RouteConfigResource>(rds_name).await);
                            self.rds_name = Some(rds_name.clone());
                        }
                    }
                }
                // Cascading watches registered above; dropping signals the xds-client to ACK.
                drop(done);
            }
            // Per gRFC A88: data errors (NACK, resource deletion) with a previously
            // cached resource are treated as ambient — keep using the cached resource
            // to avoid unnecessary outages. Downstream layers (routing, LB) retain
            // their own snapshots independently.
            ResourceEvent::ResourceChanged { result: Err(_), .. }
            | ResourceEvent::AmbientError { .. } => {}
        }
    }

    async fn handle_rds(
        &mut self,
        event: ResourceEvent<RouteConfigResource>,
        xds_client: &XdsClient,
        cache: &Arc<XdsCache>,
    ) {
        match event {
            ResourceEvent::ResourceChanged {
                result: Ok(rc),
                done,
            } => {
                self.reconcile_and_publish(rc, xds_client, cache).await;
                drop(done);
            }
            // Per gRFC A88: keep using cached resources on data errors.
            ResourceEvent::ResourceChanged { result: Err(_), .. }
            | ResourceEvent::AmbientError { .. } => {}
        }
    }

    async fn handle_cds(
        &mut self,
        cluster_name: &str,
        event: ResourceEvent<ClusterResource>,
        xds_client: &XdsClient,
        cache: &Arc<XdsCache>,
    ) {
        match event {
            ResourceEvent::ResourceChanged {
                result: Ok(cluster),
                done,
            } => {
                cache.update_cluster(cluster_name, Arc::clone(&cluster));

                let eds_name = cluster.eds_service_name().to_string();
                if self.eds_names.get(cluster_name).map(|s| s.as_str()) != Some(&eds_name) {
                    self.eds_watchers.remove(cluster_name);

                    let watcher = xds_client.watch::<EndpointsResource>(&eds_name).await;
                    let cluster_key = cluster_name.to_string();
                    self.eds_watchers
                        .insert(cluster_key.clone(), WatcherStream(watcher));
                    self.eds_names.insert(cluster_key, eds_name);
                }
                drop(done);
            }
            // Per gRFC A88: keep using cached resources on data errors.
            ResourceEvent::ResourceChanged { result: Err(_), .. }
            | ResourceEvent::AmbientError { .. } => {}
        }
    }

    fn handle_eds(
        &self,
        cluster_name: &str,
        event: ResourceEvent<EndpointsResource>,
        cache: &Arc<XdsCache>,
    ) {
        match event {
            ResourceEvent::ResourceChanged {
                result: Ok(endpoints),
                ..
            } => {
                cache.update_endpoints(cluster_name, endpoints);
            }
            // Per gRFC A88: keep using cached resources on data errors.
            ResourceEvent::ResourceChanged { result: Err(_), .. }
            | ResourceEvent::AmbientError { .. } => {}
        }
    }

    /// Reconciles per-cluster state against the route config's cluster set
    /// and publishes the config to the cache, ordered so requests never
    /// race a missing client:
    ///
    /// 1. Clients for added clusters are created first, so a request routed
    ///    on the new config always finds its client.
    /// 2. The config is published immediately after — before any watch
    ///    registration, whose sends into the ADS worker's bounded command
    ///    channel can stall and must not delay publication.
    /// 3. Removed clusters only start a [`CLUSTER_REMOVAL_GRACE`] timer;
    ///    teardown and client eviction happen in
    ///    [`process_due_removals`](Self::process_due_removals), unless the
    ///    cluster re-enters the config first.
    /// 4. CDS watches for added clusters are registered last.
    async fn reconcile_and_publish(
        &mut self,
        route_config: Arc<RouteConfigResource>,
        xds_client: &XdsClient,
        cache: &Arc<XdsCache>,
    ) {
        let new_clusters = route_config.cluster_names();
        let old_clusters: HashSet<String> = self.cds_watchers.keys().cloned().collect();

        // A cluster re-entering the config cancels its pending removal;
        // its watchers, cache entries, and warm client were never torn down.
        self.pending_removals
            .retain(|name, _| !new_clusters.contains(name));

        self.cluster_sync.add_clusters(&new_clusters);

        cache.update_route_config(route_config);

        let deadline = Instant::now() + CLUSTER_REMOVAL_GRACE;
        for name in old_clusters.difference(&new_clusters) {
            // `or_insert`: repeated configs without the cluster must not
            // keep pushing an existing deadline out.
            self.pending_removals
                .entry(name.clone())
                .or_insert(deadline);
        }

        for name in new_clusters.difference(&old_clusters) {
            let watcher = xds_client.watch::<ClusterResource>(name).await;
            self.cds_watchers
                .insert(name.clone(), WatcherStream(watcher));
        }
    }

    /// Finalizes every pending removal whose grace period has elapsed:
    /// drops its CDS/EDS watchers and cache entries — which ends the
    /// cluster's discovery streams, failing still-queued requests with
    /// UNAVAILABLE — and evicts its client from the registry.
    fn process_due_removals(&mut self, cache: &Arc<XdsCache>) {
        let now = Instant::now();
        let due: Vec<String> = self
            .pending_removals
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(name, _)| name.clone())
            .collect();
        if due.is_empty() {
            return;
        }

        for name in &due {
            self.pending_removals.remove(name);
            self.cds_watchers.remove(name);
            self.eds_watchers.remove(name);
            self.eds_names.remove(name);
            cache.remove_cluster(name);
            cache.remove_endpoints(name);
        }

        // Keep clients for everything still watched: active clusters plus
        // pending removals that are not yet due.
        let keep: HashSet<String> = self.cds_watchers.keys().cloned().collect();
        self.cluster_sync.evict_clusters(&keep);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xds::resource::route_config::{
        PathSpecifierConfig, RouteConfig, RouteConfigAction, RouteConfigMatch, VirtualHostConfig,
    };
    use xds_client::ProcessingDone;

    fn test_client() -> XdsClient {
        XdsClient::disconnected()
    }

    fn test_cache() -> Arc<XdsCache> {
        Arc::new(XdsCache::new())
    }

    /// No-op [`ActiveClusterSync`], for tests that don't care about client
    /// creation/eviction.
    struct NoopSync;

    impl ActiveClusterSync for NoopSync {
        fn add_clusters(&self, _active: &HashSet<String>) {}
        fn evict_clusters(&self, _keep: &HashSet<String>) {}
    }

    /// Records every `add_clusters` / `evict_clusters` call (sorted, for
    /// deterministic assertions).
    #[derive(Default)]
    struct RecordingSync {
        adds: std::sync::Mutex<Vec<Vec<String>>>,
        evicts: std::sync::Mutex<Vec<Vec<String>>>,
    }

    impl RecordingSync {
        fn sorted(set: &HashSet<String>) -> Vec<String> {
            let mut names: Vec<String> = set.iter().cloned().collect();
            names.sort();
            names
        }
    }

    impl ActiveClusterSync for RecordingSync {
        fn add_clusters(&self, active: &HashSet<String>) {
            self.adds.lock().unwrap().push(Self::sorted(active));
        }
        fn evict_clusters(&self, keep: &HashSet<String>) {
            self.evicts.lock().unwrap().push(Self::sorted(keep));
        }
    }

    /// `CascadeState` with a no-op cluster sync, for tests that don't care
    /// about client eviction.
    fn test_state() -> CascadeState {
        CascadeState::new(Arc::new(NoopSync))
    }

    /// Advance paused test time past the removal grace and finalize due
    /// removals, as the run loop's timer arm would.
    async fn expire_removals(state: &mut CascadeState, cache: &Arc<XdsCache>) {
        tokio::time::advance(CLUSTER_REMOVAL_GRACE + Duration::from_millis(1)).await;
        state.process_due_removals(cache);
    }

    fn make_route_config(name: &str, clusters: &[&str]) -> Arc<RouteConfigResource> {
        Arc::new(RouteConfigResource {
            name: name.into(),
            virtual_hosts: vec![VirtualHostConfig {
                name: "vh".into(),
                domains: vec!["*".into()],
                routes: clusters
                    .iter()
                    .map(|c| RouteConfig {
                        match_criteria: RouteConfigMatch {
                            path_specifier: PathSpecifierConfig::Prefix("/".into()),
                            headers: vec![],
                            case_sensitive: true,
                            match_fraction: None,
                        },
                        action: RouteConfigAction::Cluster((*c).into()),
                        retry_config: None,
                    })
                    .collect(),
            }],
            metadata: Default::default(),
        })
    }

    fn make_listener_inline(clusters: &[&str]) -> Arc<ListenerResource> {
        Arc::new(ListenerResource {
            name: "listener".into(),
            route_source: RouteSource::Inline(RouteConfigResource {
                name: "inline-rc".into(),
                virtual_hosts: vec![VirtualHostConfig {
                    name: "vh".into(),
                    domains: vec!["*".into()],
                    routes: clusters
                        .iter()
                        .map(|c| RouteConfig {
                            match_criteria: RouteConfigMatch {
                                path_specifier: PathSpecifierConfig::Prefix("/".into()),
                                headers: vec![],
                                case_sensitive: true,
                                match_fraction: None,
                            },
                            action: RouteConfigAction::Cluster((*c).into()),
                            retry_config: None,
                        })
                        .collect(),
                }],
                metadata: Default::default(),
            }),
        })
    }

    fn make_listener_rds(rds_name: &str) -> Arc<ListenerResource> {
        Arc::new(ListenerResource {
            name: "listener".into(),
            route_source: RouteSource::Rds(rds_name.into()),
        })
    }

    fn ok_event<T>(resource: Arc<T>) -> ResourceEvent<T> {
        ResourceEvent::ResourceChanged {
            result: Ok(resource),
            done: ProcessingDone::detached(),
        }
    }

    fn err_event<T>() -> ResourceEvent<T> {
        ResourceEvent::ResourceChanged {
            result: Err(xds_client::Error::ResourceDoesNotExist),
            done: ProcessingDone::detached(),
        }
    }

    fn ambient_event<T>() -> ResourceEvent<T> {
        ResourceEvent::AmbientError {
            error: xds_client::Error::ResourceDoesNotExist,
            done: ProcessingDone::detached(),
        }
    }

    #[tokio::test]
    async fn reconcile_adds_new_clusters() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        let rc = make_route_config("rc", &["a", "b"]);
        state.reconcile_and_publish(rc, &client, &cache).await;

        assert!(state.cds_watchers.contains_key("a"));
        assert!(state.cds_watchers.contains_key("b"));
        assert_eq!(state.cds_watchers.keys().count(), 2);
    }

    /// A removed cluster keeps its watchers through the grace period and is
    /// only torn down once the grace elapses.
    #[tokio::test(start_paused = true)]
    async fn reconcile_removes_old_clusters_after_grace() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        let rc1 = make_route_config("rc", &["a", "b"]);
        state.reconcile_and_publish(rc1, &client, &cache).await;

        let rc2 = make_route_config("rc", &["b", "c"]);
        state.reconcile_and_publish(rc2, &client, &cache).await;

        // Within the grace, "a" is pending removal but still watched.
        assert!(state.cds_watchers.contains_key("a"));
        assert!(state.pending_removals.contains_key("a"));
        assert_eq!(state.cds_watchers.keys().count(), 3);

        expire_removals(&mut state, &cache).await;

        assert!(!state.cds_watchers.contains_key("a"));
        assert!(state.cds_watchers.contains_key("b"));
        assert!(state.cds_watchers.contains_key("c"));
        assert_eq!(state.cds_watchers.keys().count(), 2);
        assert!(state.pending_removals.is_empty());
    }

    /// A cluster that leaves and re-enters the config within the grace
    /// keeps its watchers (and, via the sync, its warm client): the pending
    /// removal is canceled and nothing is torn down.
    #[tokio::test(start_paused = true)]
    async fn readded_cluster_cancels_pending_removal() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        state
            .reconcile_and_publish(make_route_config("rc", &["a"]), &client, &cache)
            .await;
        state
            .reconcile_and_publish(make_route_config("rc", &[]), &client, &cache)
            .await;
        assert!(state.pending_removals.contains_key("a"));

        state
            .reconcile_and_publish(make_route_config("rc", &["a"]), &client, &cache)
            .await;
        assert!(state.pending_removals.is_empty());

        expire_removals(&mut state, &cache).await;
        assert!(state.cds_watchers.contains_key("a"));
    }

    /// `add_clusters` gets the full authoritative set on every reconcile
    /// (before publication), while `evict_clusters` fires only when a
    /// removal's grace elapses, with the set of clusters to keep.
    #[tokio::test(start_paused = true)]
    async fn reconcile_adds_eagerly_and_evicts_after_grace() {
        let cache = test_cache();
        let client = test_client();
        let sync = Arc::new(RecordingSync::default());
        let mut state = CascadeState::new(sync.clone());

        state
            .reconcile_and_publish(make_route_config("rc", &["a", "b"]), &client, &cache)
            .await;
        state
            .reconcile_and_publish(make_route_config("rc", &["b"]), &client, &cache)
            .await;
        // Fires even on a no-change config.
        state
            .reconcile_and_publish(make_route_config("rc", &["b"]), &client, &cache)
            .await;

        assert_eq!(
            *sync.adds.lock().unwrap(),
            vec![
                vec!["a".to_string(), "b".to_string()],
                vec!["b".to_string()],
                vec!["b".to_string()],
            ]
        );
        // "a" is inside its grace period: no eviction yet.
        assert!(sync.evicts.lock().unwrap().is_empty());

        expire_removals(&mut state, &cache).await;
        assert_eq!(*sync.evicts.lock().unwrap(), vec![vec!["b".to_string()]]);
    }

    #[tokio::test(start_paused = true)]
    async fn reconcile_to_empty_removes_all_after_grace() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        let rc1 = make_route_config("rc", &["a"]);
        state.reconcile_and_publish(rc1, &client, &cache).await;
        assert_eq!(state.cds_watchers.keys().count(), 1);

        let rc2 = make_route_config("rc", &[]);
        state.reconcile_and_publish(rc2, &client, &cache).await;
        assert_eq!(state.cds_watchers.keys().count(), 1);

        expire_removals(&mut state, &cache).await;
        assert_eq!(state.cds_watchers.keys().count(), 0);
    }

    #[tokio::test]
    async fn handle_rds_ok_updates_cache_and_reconciles() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        let rc = make_route_config("rc-1", &["cluster-a", "cluster-b"]);
        state.handle_rds(ok_event(rc), &client, &cache).await;

        let config = cache.watch_route_config().next().await.unwrap();
        assert_eq!(config.name, "rc-1");
        assert!(state.cds_watchers.contains_key("cluster-a"));
        assert!(state.cds_watchers.contains_key("cluster-b"));
    }

    #[tokio::test]
    async fn handle_rds_err_preserves_state() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        let rc = make_route_config("rc", &["c1"]);
        state.handle_rds(ok_event(rc), &client, &cache).await;
        assert_eq!(state.cds_watchers.keys().count(), 1);

        // Per gRFC A88: data errors preserve cached state.
        state.handle_rds(err_event(), &client, &cache).await;
        assert_eq!(state.cds_watchers.keys().count(), 1);
    }

    #[tokio::test]
    async fn handle_rds_ambient_error_preserves_state() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        let rc = make_route_config("rc", &["c1"]);
        state.handle_rds(ok_event(rc), &client, &cache).await;
        assert_eq!(state.cds_watchers.keys().count(), 1);

        state.handle_rds(ambient_event(), &client, &cache).await;
        assert_eq!(state.cds_watchers.keys().count(), 1);
    }

    #[tokio::test]
    async fn handle_lds_inline_writes_route_config() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        state
            .handle_lds(ok_event(make_listener_inline(&["c1"])), &client, &cache)
            .await;

        let config = cache.watch_route_config().next().await.unwrap();
        assert_eq!(config.name, "inline-rc");
        assert!(state.rds_watcher.is_none());
        assert!(state.rds_name.is_none());
        assert!(state.cds_watchers.contains_key("c1"));
    }

    #[tokio::test]
    async fn handle_lds_inline_clears_existing_rds() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        state
            .handle_lds(ok_event(make_listener_rds("rc-1")), &client, &cache)
            .await;
        assert!(state.rds_watcher.is_some());
        assert_eq!(state.rds_name.as_deref(), Some("rc-1"));

        state
            .handle_lds(ok_event(make_listener_inline(&[])), &client, &cache)
            .await;
        assert!(state.rds_watcher.is_none());
        assert!(state.rds_name.is_none());
    }

    #[tokio::test]
    async fn handle_lds_rds_sets_watcher_and_name() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        state
            .handle_lds(ok_event(make_listener_rds("my-route")), &client, &cache)
            .await;

        assert!(state.rds_watcher.is_some());
        assert_eq!(state.rds_name.as_deref(), Some("my-route"));
    }

    #[tokio::test]
    async fn handle_lds_rds_same_name_reuses_watcher() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        state
            .handle_lds(ok_event(make_listener_rds("rc")), &client, &cache)
            .await;
        assert!(state.rds_watcher.is_some());

        // Same name — watcher should not be replaced.
        // (We can't check identity, but rds_name should stay the same.)
        state
            .handle_lds(ok_event(make_listener_rds("rc")), &client, &cache)
            .await;
        assert_eq!(state.rds_name.as_deref(), Some("rc"));
    }

    #[tokio::test]
    async fn handle_lds_rds_different_name_replaces_watcher() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        state
            .handle_lds(ok_event(make_listener_rds("rc-1")), &client, &cache)
            .await;
        assert_eq!(state.rds_name.as_deref(), Some("rc-1"));

        state
            .handle_lds(ok_event(make_listener_rds("rc-2")), &client, &cache)
            .await;
        assert_eq!(state.rds_name.as_deref(), Some("rc-2"));
    }

    #[tokio::test]
    async fn handle_lds_err_preserves_state() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        state
            .handle_lds(ok_event(make_listener_inline(&["c1"])), &client, &cache)
            .await;
        assert!(state.cds_watchers.contains_key("c1"));

        state
            .handle_lds(ok_event(make_listener_rds("rc")), &client, &cache)
            .await;
        assert!(state.rds_watcher.is_some());

        // Per gRFC A88: data errors preserve cached state.
        state.handle_lds(err_event(), &client, &cache).await;
        assert!(state.rds_watcher.is_some());
        assert_eq!(state.rds_name.as_deref(), Some("rc"));
        assert!(state.cds_watchers.contains_key("c1"));
    }

    #[tokio::test]
    async fn handle_lds_ambient_error_preserves_state() {
        let cache = test_cache();
        let client = test_client();
        let mut state = test_state();

        state
            .handle_lds(ok_event(make_listener_rds("rc")), &client, &cache)
            .await;
        assert!(state.rds_watcher.is_some());

        state.handle_lds(ambient_event(), &client, &cache).await;
        assert!(state.rds_watcher.is_some());
        assert_eq!(state.rds_name.as_deref(), Some("rc"));
    }

    /// End-to-end replica of the production Istio hang: an RDS update that
    /// references far more clusters than the xds-client worker's command
    /// channel buffers (64), reconciled through the real cascade path —
    /// `handle_rds` holds the event's `ProcessingDone` token while
    /// `reconcile_clusters` awaits one `watch()` per cluster.
    ///
    /// Before xds-client gained ADS flow control, the worker sat inside
    /// `handle_response` awaiting that token and stopped draining commands;
    /// once the command channel filled, `reconcile_clusters` blocked on the
    /// 65th watch and the client deadlocked. This test times out under that
    /// behavior and completes under the fixed worker.
    #[tokio::test]
    async fn rds_referencing_many_clusters_reconciles_without_deadlock() {
        use std::time::Duration;
        use xds_client::{
            ClientConfig, Node, ProstCodec, TokioRuntime, TonicTransportBuilder,
            XdsClient as RealXdsClient,
        };
        use xds_test_util::{XdsTestControlPlaneService, config};

        // Well past the worker's 64-slot command channel.
        const CLUSTER_COUNT: usize = 100;

        let control_plane = XdsTestControlPlaneService::new()
            .start()
            .await
            .expect("control plane failed to start");
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Rds,
            HashMap::from([(
                "rc-many".to_string(),
                config::build_route_config_with_cluster_count("rc-many", CLUSTER_COUNT),
            )]),
        );

        let client_config = ClientConfig::new(
            Node::new("test", "0"),
            format!("http://{}", control_plane.addr()),
        );
        let xds_client: RealXdsClient = RealXdsClient::builder(
            client_config,
            TonicTransportBuilder::new(),
            ProstCodec,
            TokioRuntime,
        )
        .build();

        let mut watcher = xds_client.watch::<RouteConfigResource>("rc-many").await;
        let event = tokio::time::timeout(Duration::from_secs(10), watcher.next())
            .await
            .expect("timed out waiting for the RDS update")
            .expect("watcher closed");

        let cache = test_cache();
        let mut state = test_state();
        tokio::time::timeout(
            Duration::from_secs(10),
            state.handle_rds(event, &xds_client, &cache),
        )
        .await
        .expect("deadlock: reconcile_clusters blocked while the worker awaited ProcessingDone");

        assert_eq!(state.cds_watchers.len(), CLUSTER_COUNT);
    }
}
