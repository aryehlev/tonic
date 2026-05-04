use crate::client::lb::LoadBalancingError;
use crate::xds::routing::RoutingError;
use tower::BoxError;

/// Errors that can occur when using [`XdsChannelGrpc`](crate::XdsChannelGrpc).
#[derive(Debug, thiserror::Error)]
pub enum XdsError {
    /// A routing error occurred while determining the target cluster.
    #[error("routing failed: {0}")]
    Routing(RoutingError),
    /// A load balancing error occurred.
    #[error("load balancing error: {0}")]
    LoadBalancing(LoadBalancingError),
    /// A transport-level error occurred (e.g. connection refused, HTTP/2 error).
    #[error("transport error: {0}")]
    Transport(BoxError),
}

impl XdsError {
    /// Attempt to classify a boxed error into a typed [`XdsError`].
    /// Known xDS error types are downcast; anything else becomes [`XdsError::Transport`].
    pub(crate) fn from_box_error(err: BoxError) -> Self {
        if let Some(e) = err.downcast_ref::<RoutingError>() {
            return XdsError::Routing(e.clone());
        }
        if let Some(e) = err.downcast_ref::<LoadBalancingError>() {
            return XdsError::LoadBalancing(e.clone());
        }
        XdsError::Transport(err)
    }
}

// From<XdsError> for BoxError is provided automatically by the stdlib blanket impl:
// `impl<E: Error + Send + Sync + 'static> From<E> for Box<dyn Error + Send + Sync>`
