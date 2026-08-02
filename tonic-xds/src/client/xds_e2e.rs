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
//! End-to-end tests: a real xDS channel resolving through a fake ADS control
//! plane to live gRPC backends.
//!
//! Unlike the unit tests in [`super::channel`], which inject endpoints through
//! `build_grpc_channel_from_parts`, these tests exercise the full pipeline:
//! bootstrap loading, the ADS transport, and LDS -> RDS -> CDS -> EDS resolution
//! served by [`XdsTestControlPlaneService`]. Traffic is routed to greeter (echo)
//! backends spawned by the tests.
mod test {
    use std::collections::HashMap;
    use std::net::SocketAddr;

    use tokio::time::Duration;
    use xds_test_util::RunningControlPlane;
    use xds_test_util::XdsTestControlPlaneService;
    use xds_test_util::config;

    use crate::BootstrapConfig;
    use crate::XdsChannelBuilder;
    use crate::XdsChannelConfig;
    use crate::XdsChannelGrpc;
    use crate::XdsUri;
    use crate::testutil::grpc::GreeterClient;
    use crate::testutil::grpc::HelloRequest;
    use crate::testutil::grpc::spawn_greeter_server;

    /// Starts the fake ADS control plane on an ephemeral port. Returns the
    /// running control plane (which injects config and shuts down on drop)
    /// and its address.
    async fn start_control_plane() -> (RunningControlPlane, SocketAddr) {
        let running = XdsTestControlPlaneService::new()
            .start()
            .await
            .expect("start control plane");
        let addr = running.addr();
        (running, addr)
    }

    /// Builds a real xDS channel whose bootstrap points at `cp_addr` and whose
    /// target resolves the listener `listener_name`.
    fn build_channel(cp_addr: SocketAddr, listener_name: &str) -> XdsChannelGrpc {
        let bootstrap_json = format!(
            r#"{{"xds_servers":[{{"server_uri":"http://{cp_addr}"}}],"node":{{"id":"test"}}}}"#
        );
        let bootstrap = BootstrapConfig::from_json(&bootstrap_json).expect("parse bootstrap");
        let target = XdsUri::parse(&format!("xds:///{listener_name}")).expect("parse target");
        XdsChannelBuilder::new(XdsChannelConfig::new(target).with_bootstrap(bootstrap))
            .build_grpc_channel()
            .expect("build xds channel")
    }

    /// Sends `say_hello` in a loop until a reply starting with `want_prefix` is
    /// observed (xDS resolution and config updates are asynchronous), returning
    /// that reply. Panics if it never arrives.
    async fn say_hello_until_prefix(
        client: &mut GreeterClient<XdsChannelGrpc>,
        want_prefix: &str,
    ) -> String {
        const RETRIES: usize = 100;
        let mut last = None;
        for _ in 0..RETRIES {
            match client
                .say_hello(HelloRequest {
                    name: "world".to_string(),
                })
                .await
            {
                Ok(response) => {
                    let message = response.into_inner().message;
                    if message.starts_with(want_prefix) {
                        return message;
                    }
                    last = Some(Ok(message));
                }
                Err(status) => last = Some(Err(status)),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("never observed reply starting with {want_prefix:?}; last seen: {last:?}");
    }

    /// End-to-end test: verifies that an xDS channel routes traffic to the
    /// correct backend.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xds_channel_e2e_routes_to_backend() {
        let backend = spawn_greeter_server("backend", None, None)
            .await
            .expect("spawn greeter backend");
        let backend_addr = backend.addr;

        let (control_plane, cp_addr) = start_control_plane().await;

        // Configure LDS (inline route) -> CDS -> EDS pointing at the backend.
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Lds,
            HashMap::from([(
                "my-service".to_string(),
                config::build_inline_listener("my-service", "my-cluster"),
            )]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Cds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cluster("my-cluster"),
            )]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Eds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cla(
                    "my-cluster",
                    &[(backend_addr.ip().to_string(), backend_addr.port())],
                ),
            )]),
        );

        let mut client = GreeterClient::new(build_channel(cp_addr, "my-service"));
        let reply = say_hello_until_prefix(&mut client, "backend:").await;
        assert_eq!(reply, "backend: world");

        // The control plane observed exactly one ADS stream from the client.
        let counts = control_plane.get_service().get_subscriber_counts();
        assert_eq!(counts.get(&config::AdsTypeUrl::Lds), Some(&1));
        assert_eq!(counts.get(&config::AdsTypeUrl::Cds), Some(&1));
        assert_eq!(counts.get(&config::AdsTypeUrl::Eds), Some(&1));

        let _ = backend.shutdown.send(());
    }

    /// The client is initially routing to
    /// one cluster, update the RDS route to point at a different cluster and
    /// assert traffic shifts to the new backend — exercising the control
    /// plane pushing a live update to a connected client.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xds_channel_e2e_route_update_shifts_traffic() {
        let backend_a = spawn_greeter_server("backend-a", None, None)
            .await
            .expect("spawn backend-a");
        let backend_b = spawn_greeter_server("backend-b", None, None)
            .await
            .expect("spawn backend-b");

        let (control_plane, cp_addr) = start_control_plane().await;

        // LDS -> RDS "route-config"; both clusters and their endpoints are configured
        // up front, and the route initially targets cluster-a.
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Lds,
            HashMap::from([(
                "my-service".to_string(),
                config::build_rds_listener("my-service", "route-config"),
            )]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Rds,
            HashMap::from([(
                "route-config".to_string(),
                config::build_route_config("route-config", "cluster-a"),
            )]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Cds,
            HashMap::from([
                ("cluster-a".to_string(), config::build_cluster("cluster-a")),
                ("cluster-b".to_string(), config::build_cluster("cluster-b")),
            ]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Eds,
            HashMap::from([
                (
                    "cluster-a".to_string(),
                    config::build_cla(
                        "cluster-a",
                        &[(backend_a.addr.ip().to_string(), backend_a.addr.port())],
                    ),
                ),
                (
                    "cluster-b".to_string(),
                    config::build_cla(
                        "cluster-b",
                        &[(backend_b.addr.ip().to_string(), backend_b.addr.port())],
                    ),
                ),
            ]),
        );

        let mut client = GreeterClient::new(build_channel(cp_addr, "my-service"));

        // Initially routed to cluster-a -> backend-a.
        let reply = say_hello_until_prefix(&mut client, "backend-a:").await;
        assert_eq!(reply, "backend-a: world");

        // Update the RDS route to target cluster-b; the control plane pushes the new
        // RouteConfiguration to the connected client.
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Rds,
            HashMap::from([(
                "route-config".to_string(),
                config::build_route_config("route-config", "cluster-b"),
            )]),
        );

        // Traffic shifts to cluster-b -> backend-b.
        let reply = say_hello_until_prefix(&mut client, "backend-b:").await;
        assert_eq!(reply, "backend-b: world");

        let _ = backend_a.shutdown.send(());
        let _ = backend_b.shutdown.send(());
    }

    /// P2C load balancing: with several EDS endpoints behind one cluster, traffic
    /// should spread across all of them. tonic-xds uses power-of-two-choices as
    /// its balancer (see the unit-level `test_xds_channel_grpc_with_p2c_lb`);
    /// this is the full-pipeline version through the control plane.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xds_channel_e2e_p2c_spreads_across_backends() {
        const NUM_BACKENDS: usize = 5;
        const NUM_REQUESTS: usize = 1000;

        // Spawn several greeter backends, each reporting a distinct name.
        let mut backends = Vec::new();
        for i in 0..NUM_BACKENDS {
            backends.push(
                spawn_greeter_server(&format!("backend-{i}"), None, None)
                    .await
                    .expect("spawn backend"),
            );
        }
        let endpoints: Vec<(String, u16)> = backends
            .iter()
            .map(|backend| (backend.addr.ip().to_string(), backend.addr.port()))
            .collect();

        let (control_plane, cp_addr) = start_control_plane().await;

        // Configure LDS (inline route) -> CDS -> EDS with all backends in one cluster.
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Lds,
            HashMap::from([(
                "my-service".to_string(),
                config::build_inline_listener("my-service", "my-cluster"),
            )]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Cds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cluster("my-cluster"),
            )]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Eds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cla("my-cluster", &endpoints),
            )]),
        );

        let mut client = GreeterClient::new(build_channel(cp_addr, "my-service"));

        // Warm up until xDS resolution completes and the first RPC succeeds.
        say_hello_until_prefix(&mut client, "backend-").await;

        // Tally which backend served each request.
        let mut counts: HashMap<String, usize> = HashMap::new();
        for _ in 0..NUM_REQUESTS {
            let reply = client
                .say_hello(HelloRequest {
                    name: "world".to_string(),
                })
                .await
                .expect("rpc succeeds")
                .into_inner()
                .message;
            let server = reply.split(':').next().unwrap_or_default().to_string();
            *counts.entry(server).or_default() += 1;
        }

        // Every backend should have received a fair share of the traffic.
        assert_eq!(counts.values().sum::<usize>(), NUM_REQUESTS);
        // Each backend should receive at least 50% of the fair share of requests.
        let min_per_backend = (NUM_REQUESTS / NUM_BACKENDS) / 2;
        for i in 0..NUM_BACKENDS {
            let name = format!("backend-{i}");
            let count = counts.get(&name).copied().unwrap_or(0);
            assert!(
                count >= min_per_backend,
                "backend {name} received {count} requests (< {min_per_backend}); distribution: {counts:?}",
            );
        }

        for backend in backends {
            let _ = backend.shutdown.send(());
        }
    }

    /// A black-holed endpoint (e.g. a deleted pod IP that drops SYNs) must
    /// receive no traffic: endpoints are inserted into the balancer only
    /// after their connection is established, so every request goes to the
    /// live backend from the start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xds_channel_e2e_blackholed_endpoint_gets_no_traffic() {
        let backend = spawn_greeter_server("backend", None, None)
            .await
            .expect("spawn greeter backend");
        let (control_plane, cp_addr) = start_control_plane().await;

        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Lds,
            HashMap::from([(
                "my-service".to_string(),
                config::build_inline_listener("my-service", "my-cluster"),
            )]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Cds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cluster("my-cluster"),
            )]),
        );
        // TEST-NET-1 address: unroutable, connect attempts hang until timeout.
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Eds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cla(
                    "my-cluster",
                    &[
                        ("192.0.2.1".to_string(), 50051),
                        (backend.addr.ip().to_string(), backend.addr.port()),
                    ],
                ),
            )]),
        );

        let mut client = GreeterClient::new(build_channel(cp_addr, "my-service"));
        say_hello_until_prefix(&mut client, "backend:").await;

        // Every request lands on the live backend; none pin on the black hole.
        for _ in 0..20 {
            let reply = tokio::time::timeout(
                Duration::from_secs(1),
                client.say_hello(HelloRequest {
                    name: "world".to_string(),
                }),
            )
            .await
            .expect("request pinned on a black-holed endpoint")
            .expect("request failed");
            assert_eq!(reply.into_inner().message, "backend: world");
        }

        let _ = backend.shutdown.send(());
    }

    /// Istio/Argo-style canary rollout: a weighted route across
    /// stable/canary subset clusters walks 20/40/60/80/100 while
    /// `dynamicStableScale` kills stable pods (their IPs linger black-holed
    /// in EDS for one tick, like real EDS lag) and canary pods come up.
    /// Under readiness gating no request may fail or stall.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn xds_channel_e2e_istio_canary_rollout_no_failures() {
        use envoy_types::pb::envoy::config::route::v3::route::Action;
        use envoy_types::pb::envoy::config::route::v3::route_action::ClusterSpecifier;
        use envoy_types::pb::envoy::config::route::v3::route_match::PathSpecifier;
        use envoy_types::pb::envoy::config::route::v3::weighted_cluster::ClusterWeight;
        use envoy_types::pb::envoy::config::route::v3::{
            Route, RouteAction, RouteConfiguration, RouteMatch, VirtualHost, WeightedCluster,
        };
        use envoy_types::pb::google::protobuf::UInt32Value;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        fn weighted_route(stable_weight: u32, canary_weight: u32) -> RouteConfiguration {
            // istiod drops zero-weight entries; mirror that.
            let mut clusters = Vec::new();
            if stable_weight > 0 {
                clusters.push(ClusterWeight {
                    name: "cluster-stable".to_string(),
                    weight: Some(UInt32Value {
                        value: stable_weight,
                    }),
                    ..Default::default()
                });
            }
            if canary_weight > 0 {
                clusters.push(ClusterWeight {
                    name: "cluster-canary".to_string(),
                    weight: Some(UInt32Value {
                        value: canary_weight,
                    }),
                    ..Default::default()
                });
            }
            RouteConfiguration {
                name: "route-config".to_string(),
                virtual_hosts: vec![VirtualHost {
                    name: "primary".to_string(),
                    domains: vec!["*".to_string()],
                    routes: vec![Route {
                        r#match: Some(RouteMatch {
                            path_specifier: Some(PathSpecifier::Prefix("/".to_string())),
                            ..Default::default()
                        }),
                        action: Some(Action::Route(RouteAction {
                            cluster_specifier: Some(ClusterSpecifier::WeightedClusters(
                                WeightedCluster {
                                    clusters,
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
        }

        let mut stable_pods = Vec::new();
        for i in 0..4 {
            stable_pods.push(
                spawn_greeter_server(&format!("old-{i}"), None, None)
                    .await
                    .expect("spawn stable pod"),
            );
        }
        let mut canary_pods = Vec::new();
        for i in 0..4 {
            canary_pods.push(
                spawn_greeter_server(&format!("new-{i}"), None, None)
                    .await
                    .expect("spawn canary pod"),
            );
        }
        let stable_addrs: Vec<(String, u16)> = stable_pods
            .iter()
            .map(|b| (b.addr.ip().to_string(), b.addr.port()))
            .collect();
        let canary_addrs: Vec<(String, u16)> = canary_pods
            .iter()
            .map(|b| (b.addr.ip().to_string(), b.addr.port()))
            .collect();

        let (control_plane, cp_addr) = start_control_plane().await;
        let service = control_plane.get_service().clone();
        service.set_xds_config(
            &config::AdsTypeUrl::Lds,
            HashMap::from([(
                "my-service".to_string(),
                config::build_rds_listener("my-service", "route-config"),
            )]),
        );
        service.set_xds_config(
            &config::AdsTypeUrl::Rds,
            HashMap::from([("route-config".to_string(), weighted_route(100, 0))]),
        );
        service.set_xds_config(
            &config::AdsTypeUrl::Cds,
            HashMap::from([
                (
                    "cluster-stable".to_string(),
                    config::build_cluster("cluster-stable"),
                ),
                (
                    "cluster-canary".to_string(),
                    config::build_cluster("cluster-canary"),
                ),
            ]),
        );
        service.set_xds_config(
            &config::AdsTypeUrl::Eds,
            HashMap::from([
                (
                    "cluster-stable".to_string(),
                    config::build_cla("cluster-stable", &stable_addrs),
                ),
                (
                    "cluster-canary".to_string(),
                    config::build_cla("cluster-canary", &[]),
                ),
            ]),
        );

        let channel = build_channel(cp_addr, "my-service");
        let mut warm = GreeterClient::new(channel.clone());
        say_hello_until_prefix(&mut warm, "old-").await;

        let ok = Arc::new(AtomicUsize::new(0));
        let canary_hits = Arc::new(AtomicUsize::new(0));
        let errors: Arc<std::sync::Mutex<HashMap<String, usize>>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));

        async fn worker(
            ch: XdsChannelGrpc,
            ok: Arc<AtomicUsize>,
            canary_hits: Arc<AtomicUsize>,
            errors: Arc<std::sync::Mutex<HashMap<String, usize>>>,
            stop: Arc<AtomicBool>,
        ) {
            let mut client = GreeterClient::new(ch);
            while !stop.load(Ordering::Relaxed) {
                match tokio::time::timeout(
                    Duration::from_millis(500),
                    client.say_hello(HelloRequest {
                        name: "w".to_string(),
                    }),
                )
                .await
                {
                    Ok(Ok(reply)) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                        if reply.into_inner().message.starts_with("new-") {
                            canary_hits.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(Err(status)) => {
                        *errors
                            .lock()
                            .unwrap()
                            .entry(format!("{:?}: {}", status.code(), status.message()))
                            .or_default() += 1;
                    }
                    Err(_) => {
                        *errors
                            .lock()
                            .unwrap()
                            .entry("deadline (500ms) exceeded".to_string())
                            .or_default() += 1;
                    }
                };
            }
        }

        let workers = futures_util::future::join_all((0..10).map(|_| {
            worker(
                channel.clone(),
                Arc::clone(&ok),
                Arc::clone(&canary_hits),
                Arc::clone(&errors),
                Arc::clone(&stop),
            )
        }));

        // Argo weight schedule with dynamicStableScale: at weight w, the
        // canary subset has w% of pods and the stable subset keeps the rest —
        // pods beyond that are killed, their addresses lingering black-holed
        // in the stable EDS for one tick before being removed.
        let driver = async {
            for (step, weight) in [19u32, 33, 61, 87, 100].into_iter().enumerate() {
                let canary_count = (canary_addrs.len() * weight as usize).div_ceil(100);
                // dynamicStableScale: the stable subset keeps (100-w)% of its
                // replicas (never zero while it still carries route weight).
                let stable_count = (stable_addrs.len() * (100 - weight) as usize).div_ceil(100);

                // Stable EDS: surviving pods, plus one tick of black-holed
                // entries standing in for pods that died abruptly (their EDS
                // removal lags their death, as in a real rollout).
                let mut stable_set: Vec<(String, u16)> = stable_addrs[..stable_count].to_vec();
                for k in 0..(stable_addrs.len() - stable_count) {
                    stable_set.push((format!("192.0.2.{}", step * 4 + k + 1), 50051));
                }

                service.set_xds_config(
                    &config::AdsTypeUrl::Rds,
                    HashMap::from([(
                        "route-config".to_string(),
                        weighted_route(100 - weight, weight),
                    )]),
                );
                service.set_xds_config(
                    &config::AdsTypeUrl::Eds,
                    HashMap::from([
                        (
                            "cluster-stable".to_string(),
                            config::build_cla("cluster-stable", &stable_set),
                        ),
                        (
                            "cluster-canary".to_string(),
                            config::build_cla("cluster-canary", &canary_addrs[..canary_count]),
                        ),
                    ]),
                );
                tokio::time::sleep(Duration::from_millis(300)).await;

                // Gracefully-drained pods die only after leaving EDS and
                // letting in-flight requests complete (k8s preStop ordering).
                for pod in stable_pods.drain(stable_count.min(stable_pods.len())..) {
                    let _ = pod.shutdown.send(());
                }

                // EDS lag over: drop the black-holed entries.
                service.set_xds_config(
                    &config::AdsTypeUrl::Eds,
                    HashMap::from([
                        (
                            "cluster-stable".to_string(),
                            config::build_cla("cluster-stable", &stable_addrs[..stable_count]),
                        ),
                        (
                            "cluster-canary".to_string(),
                            config::build_cla("cluster-canary", &canary_addrs[..canary_count]),
                        ),
                    ]),
                );
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            stop.store(true, Ordering::Relaxed);
        };
        tokio::join!(workers, driver);

        let ok = ok.load(Ordering::Relaxed);
        let canary_hits = canary_hits.load(Ordering::Relaxed);
        let errors = errors.lock().unwrap();
        let failed: usize = errors.values().sum();
        println!(
            "canary rollout: ok={ok} canary_hits={canary_hits} failed={failed} errors={errors:?}"
        );
        assert!(ok > 0, "no successful requests during the rollout");
        assert!(
            canary_hits > 0,
            "traffic never shifted to canary pods ({ok} ok)"
        );
        assert_eq!(
            failed, 0,
            "requests failed or stalled during the canary rollout \
             ({ok} ok, {canary_hits} on canary, errors: {errors:?})"
        );

        for pod in stable_pods {
            let _ = pod.shutdown.send(());
        }
        for pod in canary_pods {
            let _ = pod.shutdown.send(());
        }
        drop(control_plane);
    }

    /// `with_request_timeout` bounds a request that would otherwise wait
    /// forever — here, a cluster whose only endpoint never connects.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xds_channel_e2e_request_timeout_bounds_stalled_requests() {
        let (control_plane, cp_addr) = start_control_plane().await;
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Lds,
            HashMap::from([(
                "my-service".to_string(),
                config::build_inline_listener("my-service", "my-cluster"),
            )]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Cds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cluster("my-cluster"),
            )]),
        );
        control_plane.get_service().set_xds_config(
            &config::AdsTypeUrl::Eds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cla("my-cluster", &[("192.0.2.1".to_string(), 50051)]),
            )]),
        );

        let bootstrap_json = format!(
            r#"{{"xds_servers":[{{"server_uri":"http://{cp_addr}"}}],"node":{{"id":"test"}}}}"#
        );
        let bootstrap = BootstrapConfig::from_json(&bootstrap_json).expect("parse bootstrap");
        let target = XdsUri::parse("xds:///my-service").expect("parse target");
        let channel = XdsChannelBuilder::new(
            XdsChannelConfig::new(target)
                .with_bootstrap(bootstrap)
                .with_request_timeout(Duration::from_millis(200)),
        )
        .build_grpc_channel()
        .expect("build xds channel");

        let mut client = GreeterClient::new(channel);
        let started = std::time::Instant::now();
        let result = client
            .say_hello(HelloRequest {
                name: "world".to_string(),
            })
            .await;
        assert!(result.is_err(), "request to a dead cluster cannot succeed");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "request timeout did not bound the stalled request ({:?})",
            started.elapsed()
        );

        drop(control_plane);
    }

    /// Full-outage guard: sustained churn minting fresh black-holed EDS
    /// entries, 200 deadline-less workers. Pre-readiness-gating this
    /// collapses to zero throughput and wedges permanently; now every
    /// second must serve traffic and every request must succeed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn xds_channel_e2e_churn_full_outage_guard() {
        use std::sync::Arc;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut backends = Vec::new();
        for i in 0..4 {
            backends.push(
                spawn_greeter_server(&format!("pod-{i}"), None, None)
                    .await
                    .expect("spawn backend"),
            );
        }
        let live: Vec<(String, u16)> = backends
            .iter()
            .map(|b| (b.addr.ip().to_string(), b.addr.port()))
            .collect();

        let (control_plane, cp_addr) = start_control_plane().await;
        let service = control_plane.get_service().clone();
        service.set_xds_config(
            &config::AdsTypeUrl::Lds,
            HashMap::from([(
                "my-service".to_string(),
                config::build_inline_listener("my-service", "my-cluster"),
            )]),
        );
        service.set_xds_config(
            &config::AdsTypeUrl::Cds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cluster("my-cluster"),
            )]),
        );
        service.set_xds_config(
            &config::AdsTypeUrl::Eds,
            HashMap::from([(
                "my-cluster".to_string(),
                config::build_cla("my-cluster", &live),
            )]),
        );

        let channel = build_channel(cp_addr, "my-service");
        let mut warm = GreeterClient::new(channel.clone());
        say_hello_until_prefix(&mut warm, "pod-").await;

        let events: Arc<Mutex<Vec<(u128, bool)>>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));
        let t0 = std::time::Instant::now();

        // No per-request timeout, like a caller that sets no deadline.
        async fn worker(
            ch: XdsChannelGrpc,
            events: Arc<Mutex<Vec<(u128, bool)>>>,
            stop: Arc<AtomicBool>,
            t0: std::time::Instant,
        ) {
            let mut client = GreeterClient::new(ch);
            while !stop.load(Ordering::Relaxed) {
                let ok = client
                    .say_hello(HelloRequest {
                        name: "w".to_string(),
                    })
                    .await
                    .is_ok();
                events.lock().unwrap().push((t0.elapsed().as_millis(), ok));
            }
        }

        let workers = futures_util::future::join_all(
            (0..200).map(|_| worker(channel.clone(), Arc::clone(&events), Arc::clone(&stop), t0)),
        );

        let driver = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            // 4s of churn: each tick keeps the live pods and mints four
            // fresh black-holed entries (new addresses, zero recorded load).
            for tick in 0u16..13 {
                let mut set = live.clone();
                for k in 0..4 {
                    set.push((format!("192.0.2.{}", (tick * 4 + k) % 250 + 1), 50051));
                }
                service.set_xds_config(
                    &config::AdsTypeUrl::Eds,
                    HashMap::from([(
                        "my-cluster".to_string(),
                        config::build_cla("my-cluster", &set),
                    )]),
                );
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            service.set_xds_config(
                &config::AdsTypeUrl::Eds,
                HashMap::from([(
                    "my-cluster".to_string(),
                    config::build_cla("my-cluster", &live),
                )]),
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
            stop.store(true, Ordering::Relaxed);
        };

        // The wedged (pre-fix) failure mode never finishes; bound it so a
        // regression fails instead of hanging the suite.
        tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(workers, driver);
        })
        .await
        .expect("full outage: workers wedged on dead endpoints and never completed");

        let events = events.lock().unwrap();
        let total_secs = t0.elapsed().as_secs() as u128;
        let mut buckets: std::collections::BTreeMap<u128, (usize, usize)> =
            std::collections::BTreeMap::new();
        for (at, ok) in events.iter() {
            let e = buckets.entry(at / 1000).or_default();
            if *ok { e.0 += 1 } else { e.1 += 1 }
        }
        let failed: usize = buckets.values().map(|(_, err)| err).sum();
        assert_eq!(failed, 0, "requests failed during churn: {buckets:?}");
        // Every whole second must have served traffic (skip the final
        // partial second).
        for sec in 0..total_secs.saturating_sub(1) {
            let ok = buckets.get(&sec).map(|(ok, _)| *ok).unwrap_or(0);
            assert!(
                ok > 0,
                "zero throughput during second {sec} (full outage): {buckets:?}"
            );
        }

        for b in backends {
            let _ = b.shutdown.send(());
        }
        drop(control_plane);
    }

    /// A cluster removed from the route config and later re-added (an Argo
    /// canary between rollouts — istiod drops zero-weight route entries) must
    /// resolve fresh endpoints, not resurrect a client whose discovery died
    /// with the old cache entry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xds_channel_e2e_cluster_removed_and_readded_recovers() {
        use envoy_types::pb::envoy::config::route::v3::route::Action;
        use envoy_types::pb::envoy::config::route::v3::route_action::ClusterSpecifier;
        use envoy_types::pb::envoy::config::route::v3::route_match::PathSpecifier;
        use envoy_types::pb::envoy::config::route::v3::weighted_cluster::ClusterWeight;
        use envoy_types::pb::envoy::config::route::v3::{
            Route, RouteAction, RouteConfiguration, RouteMatch, VirtualHost, WeightedCluster,
        };
        use envoy_types::pb::google::protobuf::UInt32Value;

        fn weighted_route(weights: &[(&str, u32)]) -> RouteConfiguration {
            RouteConfiguration {
                name: "route-config".to_string(),
                virtual_hosts: vec![VirtualHost {
                    name: "primary".to_string(),
                    domains: vec!["*".to_string()],
                    routes: vec![Route {
                        r#match: Some(RouteMatch {
                            path_specifier: Some(PathSpecifier::Prefix("/".to_string())),
                            ..Default::default()
                        }),
                        action: Some(Action::Route(RouteAction {
                            cluster_specifier: Some(ClusterSpecifier::WeightedClusters(
                                WeightedCluster {
                                    clusters: weights
                                        .iter()
                                        .map(|(name, weight)| ClusterWeight {
                                            name: (*name).to_string(),
                                            weight: Some(UInt32Value { value: *weight }),
                                            ..Default::default()
                                        })
                                        .collect(),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
        }

        let stable = spawn_greeter_server("stable", None, None)
            .await
            .expect("spawn stable");
        let canary_old = spawn_greeter_server("canary-old", None, None)
            .await
            .expect("spawn old canary");
        let canary_new = spawn_greeter_server("canary-new", None, None)
            .await
            .expect("spawn new canary");

        let (control_plane, cp_addr) = start_control_plane().await;
        let service = control_plane.get_service().clone();
        service.set_xds_config(
            &config::AdsTypeUrl::Lds,
            HashMap::from([(
                "my-service".to_string(),
                config::build_rds_listener("my-service", "route-config"),
            )]),
        );
        service.set_xds_config(
            &config::AdsTypeUrl::Rds,
            HashMap::from([(
                "route-config".to_string(),
                weighted_route(&[("cluster-stable", 50), ("cluster-canary", 50)]),
            )]),
        );
        service.set_xds_config(
            &config::AdsTypeUrl::Cds,
            HashMap::from([
                (
                    "cluster-stable".to_string(),
                    config::build_cluster("cluster-stable"),
                ),
                (
                    "cluster-canary".to_string(),
                    config::build_cluster("cluster-canary"),
                ),
            ]),
        );
        service.set_xds_config(
            &config::AdsTypeUrl::Eds,
            HashMap::from([
                (
                    "cluster-stable".to_string(),
                    config::build_cla(
                        "cluster-stable",
                        &[(stable.addr.ip().to_string(), stable.addr.port())],
                    ),
                ),
                (
                    "cluster-canary".to_string(),
                    config::build_cla(
                        "cluster-canary",
                        &[(canary_old.addr.ip().to_string(), canary_old.addr.port())],
                    ),
                ),
            ]),
        );

        let mut client = GreeterClient::new(build_channel(cp_addr, "my-service"));
        say_hello_until_prefix(&mut client, "canary-old:").await;

        // Rollout completes: the canary cluster leaves the route entirely
        // (istiod drops zero-weight destinations).
        service.set_xds_config(
            &config::AdsTypeUrl::Rds,
            HashMap::from([(
                "route-config".to_string(),
                weighted_route(&[("cluster-stable", 100)]),
            )]),
        );
        say_hello_until_prefix(&mut client, "stable:").await;
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Between rollouts the old canary pods die; the next rollout brings
        // new ones.
        let _ = canary_old.shutdown.send(());
        service.set_xds_config(
            &config::AdsTypeUrl::Eds,
            HashMap::from([
                (
                    "cluster-stable".to_string(),
                    config::build_cla(
                        "cluster-stable",
                        &[(stable.addr.ip().to_string(), stable.addr.port())],
                    ),
                ),
                (
                    "cluster-canary".to_string(),
                    config::build_cla(
                        "cluster-canary",
                        &[(canary_new.addr.ip().to_string(), canary_new.addr.port())],
                    ),
                ),
            ]),
        );

        // Next rollout: the canary cluster returns.
        service.set_xds_config(
            &config::AdsTypeUrl::Rds,
            HashMap::from([(
                "route-config".to_string(),
                weighted_route(&[("cluster-stable", 50), ("cluster-canary", 50)]),
            )]),
        );

        // The resurrected cluster must serve from the NEW canary pod.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(Ok(reply)) = tokio::time::timeout(
                Duration::from_millis(500),
                client.say_hello(HelloRequest {
                    name: "world".to_string(),
                }),
            )
            .await
                && reply.into_inner().message.starts_with("canary-new:")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "re-added cluster never served from its new endpoints \
                 (stale ClusterClient with dead discovery)"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _ = stable.shutdown.send(());
        let _ = canary_new.shutdown.send(());
        drop(control_plane);
    }
}
