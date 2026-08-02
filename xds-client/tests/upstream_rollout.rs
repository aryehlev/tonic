//! Reproduces stale pod discovery after an upstream rollout, and shows the
//! ADS keepalive fix recovering from it.
//!
//! The scenario: the client holds an ADS stream to an xDS server (e.g. istiod)
//! and has discovered the pods of a backend cluster via EDS. The node or pod
//! hosting the xDS connection is then rolled: packets on the established TCP
//! connection are silently dropped (no RST/FIN reaches the client — the
//! conntrack/NAT entry just dangles), while *new* connections reach the
//! replacement server fine. The backend is rolled too, so its pod IPs change.
//!
//! Without HTTP/2 keepalives on the ADS channel (the behavior before commit
//! a9c8cb8b), `recv()` on the dead stream pends forever: the client never
//! learns the new pod IPs and keeps routing to deleted ones. With keepalives,
//! the unanswered PING surfaces a transport error, the worker reconnects and
//! re-subscribes, and discovery converges on the new pods.
//!
//! The rollout is simulated with a TCP proxy that can "black-hole" the
//! connections established before a cutover point while accepting new ones.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use envoy_types::pb::envoy::config::core::v3 as core_v3;
use envoy_types::pb::envoy::config::endpoint::v3::{
    ClusterLoadAssignment, LbEndpoint, LocalityLbEndpoints, lb_endpoint::HostIdentifier,
};
use envoy_types::pb::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse,
    aggregated_discovery_service_server::{
        AggregatedDiscoveryService, AggregatedDiscoveryServiceServer,
    },
};
use envoy_types::pb::google::protobuf::Any;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt, wrappers::TcpListenerStream};
use tonic::{Request, Response, Status};

use xds_client::resource::TypeUrl;
use xds_client::{
    ClientConfig, Node, ProstCodec, Resource, ResourceEvent, RetryPolicy, TokioRuntime,
    TonicTransportBuilder, XdsClient,
};

const EDS_TYPE_URL: &str = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";
const CLUSTER: &str = "backend";

/// Pod addresses discovered for a cluster, extracted from EDS.
#[derive(Debug, Clone)]
struct Pods {
    addrs: Vec<String>,
}

impl Resource for Pods {
    type Message = ClusterLoadAssignment;

    const TYPE_URL: TypeUrl = TypeUrl::new(EDS_TYPE_URL);

    fn deserialize(bytes: Bytes) -> xds_client::Result<Self::Message> {
        ClusterLoadAssignment::decode(bytes).map_err(Into::into)
    }

    fn name(message: &Self::Message) -> &str {
        &message.cluster_name
    }

    fn validate(message: Self::Message) -> xds_client::Result<Self> {
        let addrs = message
            .endpoints
            .iter()
            .flat_map(|le| &le.lb_endpoints)
            .filter_map(|lb| match &lb.host_identifier {
                Some(HostIdentifier::Endpoint(ep)) => ep.address.as_ref(),
                _ => None,
            })
            .filter_map(|addr| match &addr.address {
                Some(core_v3::address::Address::SocketAddress(sa)) => {
                    let port = match sa.port_specifier {
                        Some(core_v3::socket_address::PortSpecifier::PortValue(p)) => p,
                        _ => 0,
                    };
                    Some(format!("{}:{port}", sa.address))
                }
                _ => None,
            })
            .collect();
        Ok(Self { addrs })
    }
}

/// The cluster's pod set, versioned so the test can roll it.
type PodSet = (u64, Vec<(&'static str, u32)>);

fn cla_response(version: u64, pods: &[(&'static str, u32)], nonce: u64) -> DiscoveryResponse {
    let cla = ClusterLoadAssignment {
        cluster_name: CLUSTER.to_string(),
        endpoints: vec![LocalityLbEndpoints {
            lb_endpoints: pods
                .iter()
                .map(|(host, port)| LbEndpoint {
                    host_identifier: Some(HostIdentifier::Endpoint(
                        envoy_types::pb::envoy::config::endpoint::v3::Endpoint {
                            address: Some(core_v3::Address {
                                address: Some(core_v3::address::Address::SocketAddress(
                                    core_v3::SocketAddress {
                                        address: host.to_string(),
                                        port_specifier: Some(
                                            core_v3::socket_address::PortSpecifier::PortValue(
                                                *port,
                                            ),
                                        ),
                                        ..Default::default()
                                    },
                                )),
                            }),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    DiscoveryResponse {
        version_info: version.to_string(),
        type_url: EDS_TYPE_URL.to_string(),
        nonce: nonce.to_string(),
        resources: vec![Any {
            type_url: EDS_TYPE_URL.to_string(),
            value: cla.encode_to_vec(),
        }],
        ..Default::default()
    }
}

/// EDS server whose pod set can be rolled at runtime via a watch channel.
///
/// Each stream sends the current pod set on the first EDS request (a fresh
/// stream is a fresh subscription, whatever version_info the client resumes
/// with), pushes again whenever the pod set changes, and ignores ACKs.
struct RollingEdsServer {
    pods: watch::Receiver<PodSet>,
}

#[tonic::async_trait]
impl AggregatedDiscoveryService for RollingEdsServer {
    type StreamAggregatedResourcesStream =
        Pin<Box<dyn Stream<Item = Result<DiscoveryResponse, Status>> + Send>>;

    async fn stream_aggregated_resources(
        &self,
        request: Request<tonic::Streaming<DiscoveryRequest>>,
    ) -> Result<Response<Self::StreamAggregatedResourcesStream>, Status> {
        let mut inbound = request.into_inner();
        let mut pods = self.pods.clone();

        let outbound = async_stream::try_stream! {
            let mut subscribed = false;
            let mut last_sent: Option<u64> = None;
            let mut nonce = 0u64;
            loop {
                tokio::select! {
                    req = inbound.next() => {
                        let Some(Ok(req)) = req else { break };
                        // First EDS request on this stream is the
                        // subscription; everything after is an ACK.
                        if req.type_url == EDS_TYPE_URL && !subscribed {
                            eprintln!("[server] EDS subscription (version_info={:?})", req.version_info);
                            subscribed = true;
                            let (version, set) = pods.borrow_and_update().clone();
                            nonce += 1;
                            last_sent = Some(version);
                            yield cla_response(version, &set, nonce);
                        }
                    }
                    changed = pods.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        if subscribed {
                            let (version, set) = pods.borrow_and_update().clone();
                            if last_sent != Some(version) {
                                nonce += 1;
                                last_sent = Some(version);
                                yield cla_response(version, &set, nonce);
                            }
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(outbound)))
    }

    type DeltaAggregatedResourcesStream =
        Pin<Box<dyn Stream<Item = Result<DeltaDiscoveryResponse, Status>> + Send>>;

    async fn delta_aggregated_resources(
        &self,
        _request: Request<tonic::Streaming<DeltaDiscoveryRequest>>,
    ) -> Result<Response<Self::DeltaAggregatedResourcesStream>, Status> {
        Err(Status::unimplemented("delta not supported"))
    }
}

async fn start_eds_server(pods: watch::Receiver<PodSet>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(AggregatedDiscoveryServiceServer::new(RollingEdsServer {
                pods,
            }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    addr
}

/// TCP proxy that simulates the network side of a rollout.
///
/// `roll()` bumps a generation counter: connections accepted at an older
/// generation stop forwarding bytes in either direction but their sockets
/// stay open — exactly what the client sees when the peer's node goes away
/// and packets are dropped without an RST. Connections accepted after the
/// bump forward normally (a fresh connect reaches the replacement server).
struct RolloutProxy {
    addr: SocketAddr,
    generation: Arc<AtomicU64>,
}

impl RolloutProxy {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let generation = Arc::new(AtomicU64::new(0));

        let generation_accept = generation.clone();
        tokio::spawn(async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else {
                    break;
                };
                let Ok(outbound) = tokio::net::TcpStream::connect(upstream).await else {
                    continue;
                };
                let conn_generation = generation_accept.load(Ordering::SeqCst);
                eprintln!("[proxy] accepted connection at generation {conn_generation}");
                let (in_read, in_write) = inbound.into_split();
                let (out_read, out_write) = outbound.into_split();
                tokio::spawn(forward(
                    in_read,
                    out_write,
                    conn_generation,
                    generation_accept.clone(),
                    "c->s",
                ));
                tokio::spawn(forward(
                    out_read,
                    in_write,
                    conn_generation,
                    generation_accept.clone(),
                    "s->c",
                ));
            }
        });

        Self { addr, generation }
    }

    /// Black-hole every connection established so far.
    fn roll(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }
}

async fn forward(
    mut read: OwnedReadHalf,
    mut write: OwnedWriteHalf,
    conn_generation: u64,
    generation: Arc<AtomicU64>,
    dir: &'static str,
) {
    let mut buf = [0u8; 16 * 1024];
    loop {
        match read.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                eprintln!(
                    "[proxy gen{conn_generation} {dir}] {n} bytes at {:?}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .subsec_millis()
                );
                // Re-check after the read: bytes that arrive once the
                // connection is rolled are dropped (lost packets), and the
                // sockets stay open so neither side ever sees a FIN/RST.
                if generation.load(Ordering::SeqCst) > conn_generation {
                    std::future::pending::<()>().await;
                }
                if write.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    }
}

const OLD_PODS: [(&str, u32); 2] = [("10.0.0.1", 8080), ("10.0.0.2", 8080)];
const NEW_PODS: [(&str, u32); 2] = [("10.0.1.7", 8080), ("10.0.1.8", 8080)];

fn addrs(pods: &[(&str, u32)]) -> Vec<String> {
    pods.iter().map(|(h, p)| format!("{h}:{p}")).collect()
}

/// Drive the rollout scenario and report whether the client discovered the
/// new pods within `deadline`.
///
/// `keep_alive = None` reproduces the ADS channel as it was before commit
/// a9c8cb8b ("add timeout for connections."): no HTTP/2 keepalives, so a
/// black-holed connection is undetectable.
async fn discovers_new_pods_after_rollout(
    keep_alive: Option<(Duration, Duration)>,
    deadline: Duration,
) -> bool {
    let (pods_tx, pods_rx) = watch::channel::<PodSet>((1, OLD_PODS.to_vec()));
    let server_addr = start_eds_server(pods_rx).await;
    let proxy = RolloutProxy::start(server_addr).await;

    let node = Node::new("grpc", "1.0").with_id("rollout-test-node");
    let config = ClientConfig::new(node, format!("http://{}", proxy.addr)).with_retry_policy(
        RetryPolicy::new(Duration::from_millis(100), Duration::from_millis(500), 2.0).unwrap(),
    );

    let transport = match keep_alive {
        Some((interval, timeout)) => {
            TonicTransportBuilder::new().with_keep_alive(Some(interval), timeout)
        }
        None => TonicTransportBuilder::new().with_keep_alive(None, Duration::from_secs(1)),
    };

    let client = XdsClient::builder(config, transport, ProstCodec, TokioRuntime).build();
    let mut watcher = client.watch::<Pods>(CLUSTER).await;

    // Steady state: the old pods are discovered over the ADS stream.
    let first = tokio::time::timeout(Duration::from_secs(5), watcher.next())
        .await
        .expect("timed out waiting for initial pods")
        .expect("watcher closed");
    match first {
        ResourceEvent::ResourceChanged {
            result: Ok(pods),
            done,
        } => {
            // Release `done` now: the worker does not read the next response
            // until every watcher signals ProcessingDone (ADS flow control),
            // and a `..` pattern would keep it alive until the end of this
            // function.
            drop(done);
            assert_eq!(pods.addrs, addrs(&OLD_PODS));
        }
        other => panic!("expected initial pod set, got {other:?}"),
    }

    // Let the ADS session settle (ACK flushed, keepalive cadence established)
    // before the rollout, as in the real scenario where the connection has
    // been idle long before the upstream rolls.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // The rollout: the established ADS connection goes dark without an RST,
    // and the backend comes up with new pod IPs that the (replacement) xDS
    // server knows about.
    proxy.roll();
    tokio::time::sleep(Duration::from_millis(100)).await;
    pods_tx.send((2, NEW_PODS.to_vec())).unwrap();

    // Did discovery converge on the new pods in time?
    let result = tokio::time::timeout(deadline, async {
        while let Some(event) = watcher.next().await {
            eprintln!("[test] event after rollout: {event:?}");
            if let ResourceEvent::ResourceChanged {
                result: Ok(pods), ..
            } = event
                && pods.addrs == addrs(&NEW_PODS)
            {
                return;
            }
            // Ambient errors (the reconnects) and stale re-deliveries are
            // fine; keep waiting for the new pod set.
        }
        panic!("watcher closed before the new pods were discovered");
    })
    .await
    .is_ok();

    drop(client);
    result
}

/// Pre-fix behavior (no ADS keepalives, as before commit a9c8c8b): after the
/// rollout the client sits on the dead stream forever and keeps the deleted
/// pod IPs — discovery is silently ruined.
#[tokio::test]
async fn upstream_rollout_leaves_stale_pods_without_keepalive() {
    let discovered = discovers_new_pods_after_rollout(None, Duration::from_secs(5)).await;
    assert!(
        !discovered,
        "without keepalives the dead ADS connection should never be detected, \
         yet the client discovered the new pods — has liveness detection been \
         added elsewhere?"
    );
}

/// With the fix: keepalive PINGs go unanswered on the black-holed connection,
/// the transport errors out, the worker reconnects and re-subscribes, and the
/// new pods are discovered. (Intervals are shortened from the 30s/10s
/// defaults to keep the test fast.)
#[tokio::test(flavor = "multi_thread")]
async fn upstream_rollout_recovers_with_keepalive() {
    let discovered = discovers_new_pods_after_rollout(
        Some((Duration::from_millis(500), Duration::from_millis(500))),
        Duration::from_secs(10),
    )
    .await;
    assert!(
        discovered,
        "keepalives should surface the black-holed ADS connection as a \
         transport error and reconnection should rediscover the new pods"
    );
}
