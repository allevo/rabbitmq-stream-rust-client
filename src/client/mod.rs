use std::ops::DerefMut;
use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use std::{future::Future, sync::atomic::Ordering};

use futures::{
    stream::{SplitSink, SplitStream},
    Stream, StreamExt, TryFutureExt,
};
use pin_project::pin_project;
use rabbitmq_stream_protocol::commands::exchange_command_versions::{
    ExchangeCommandVersionsRequest, ExchangeCommandVersionsResponse,
};
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::RwLock;
use tokio::{net::TcpStream, sync::Notify};
use tokio_rustls::client::TlsStream;

use tokio_util::codec::Framed;
use tracing::trace;

use crate::{error::ClientError, RabbitMQStreamResult};
pub use message::ClientMessage;
pub use metadata::{Broker, StreamMetadata};
pub use metrics::MetricsCollector;
pub use options::{ClientOptions, TlsConfiguration, TlsConfigurationBuilder};
use rabbitmq_stream_protocol::{
    commands::{
        close::{CloseRequest, CloseResponse},
        consumer_update_request::ConsumerUpdateRequestCommand,
        create_stream::CreateStreamCommand,
        create_super_stream::CreateSuperStreamCommand,
        credit::CreditCommand,
        declare_publisher::DeclarePublisherCommand,
        delete::Delete,
        delete_publisher::DeletePublisherCommand,
        delete_super_stream::DeleteSuperStreamCommand,
        generic::GenericResponse,
        metadata::MetadataCommand,
        open::{OpenCommand, OpenResponse},
        peer_properties::{PeerPropertiesCommand, PeerPropertiesResponse},
        publish::PublishCommand,
        query_offset::{QueryOffsetRequest, QueryOffsetResponse},
        query_publisher_sequence::{QueryPublisherRequest, QueryPublisherResponse},
        sasl_authenticate::SaslAuthenticateCommand,
        sasl_handshake::{SaslHandshakeCommand, SaslHandshakeResponse},
        store_offset::StoreOffset,
        subscribe::{OffsetSpecification, SubscribeCommand},
        superstream_partitions::SuperStreamPartitionsRequest,
        superstream_partitions::SuperStreamPartitionsResponse,
        superstream_route::SuperStreamRouteRequest,
        superstream_route::SuperStreamRouteResponse,
        unsubscribe::UnSubscribeCommand,
    },
    types::PublishedMessage,
    FromResponse, Request, Response, ResponseCode, ResponseKind,
};

pub use self::handler::{MessageHandler, MessageResult};
use self::{
    channel::{channel, ChannelReceiver, ChannelSender},
    codec::RabbitMqStreamCodec,
    dispatcher::Dispatcher,
    message::BaseMessage,
};

mod channel;
mod codec;
mod connection;
mod dispatcher;
mod handler;
mod message;
pub mod metadata;
mod metrics;
mod options;
mod task;

pub use connection::*;

#[cfg_attr(docsrs, doc(cfg(feature = "tokio-stream")))]
#[pin_project(project = StreamProj)]
#[derive(Debug)]
pub enum GenericTcpStream {
    Tcp(#[pin] TcpStream),
    SecureTcp(#[pin] TlsStream<TcpStream>),
}

impl AsyncRead for GenericTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            StreamProj::Tcp(tcp_stream) => tcp_stream.poll_read(cx, buf),
            StreamProj::SecureTcp(tls_stream) => tls_stream.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for GenericTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            StreamProj::Tcp(tcp_stream) => tcp_stream.poll_write(cx, buf),
            StreamProj::SecureTcp(tls_stream) => tls_stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            StreamProj::Tcp(tcp_stream) => tcp_stream.poll_flush(cx),
            StreamProj::SecureTcp(tls_stream) => tls_stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            StreamProj::Tcp(tcp_stream) => tcp_stream.poll_shutdown(cx),
            StreamProj::SecureTcp(tls_stream) => tls_stream.poll_shutdown(cx),
        }
    }
}

type SinkConnection = SplitSink<Framed<GenericTcpStream, RabbitMqStreamCodec>, Request>;
type StreamConnection = SplitStream<Framed<GenericTcpStream, RabbitMqStreamCodec>>;

pub struct ClientState {
    server_properties: HashMap<String, String>,
    connection_properties: HashMap<String, String>,
    heartbeat: u32,
    max_frame_size: u32,
    last_heatbeat: Instant,
    heartbeat_task: Option<task::TaskHandle>,
}

#[async_trait::async_trait]
impl MessageHandler for Client {
    async fn handle_message(&self, item: MessageResult) -> RabbitMQStreamResult<()> {
        match &item {
            Some(Ok(response)) => match response.kind_ref() {
                _ => {
                    if let Some(handler) = self.handler.read().await.as_ref() {
                        let handler = handler.clone();
                        tokio::task::spawn(async move { handler.handle_message(item).await });
                    }
                }
            },
            Some(Err(err)) => {
                trace!(?err);
                if let Some(handler) = self.handler.read().await.as_ref() {
                    let handler = handler.clone();

                    tokio::task::spawn(async move { handler.handle_message(item).await });
                }
            }
            None => {
                trace!("Closing client");
                if let Some(handler) = self.handler.read().await.as_ref() {
                    let handler = handler.clone();
                    tokio::task::spawn(async move { handler.handle_message(None).await });
                }
            }
        }

        Ok(())
    }
}

/// Raw API for taking to RabbitMQ stream
///
/// For high level APIs check [`crate::Environment`]
#[derive(Clone)]
pub struct Client {
    opts: ClientOptions,
    connection: Arc<Connection>,
    handler: Arc<RwLock<Option<Arc<dyn MessageHandler>>>>,
}

impl Client {
    pub async fn connect(opts: impl Into<ClientOptions>) -> Result<Client, ClientError> {
        let opts = opts.into();
        let connection = Connection::connect(opts.clone()).await?;

        let client = Client {
            opts,
            connection,
            handler: Arc::new(RwLock::new(None)),
        };

        client.set_handler(client.clone()).await;

        Ok(client)
    }

    /// Get client's server properties.
    pub fn server_properties(&self) -> HashMap<String, String> {
        self.connection.server_properties().clone()
    }

    /// Get client's connection properties.
    pub fn connection_properties(&self) -> HashMap<String, String> {
        self.connection.connection_properties().clone()
    }

    pub async fn set_handler<H: MessageHandler>(&self, handler: H) {
        let mut h = self.handler.write().await;
        *h = Some(Arc::new(handler));
    }

    pub async fn close(&self) -> RabbitMQStreamResult<()> {
        self.connection.close().await
    }
    pub async fn subscribe(
        &self,
        subscription_id: u8,
        stream: &str,
        offset_specification: OffsetSpecification,
        credit: u16,
        properties: HashMap<String, String>,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.connection.send_and_receive(|correlation_id| {
            SubscribeCommand::new(
                correlation_id,
                subscription_id,
                stream.to_owned(),
                offset_specification,
                credit,
                properties,
            )
        })
        .await
    }

    pub async fn unsubscribe(&self, subscription_id: u8) -> RabbitMQStreamResult<GenericResponse> {
        self.connection.send_and_receive(|correlation_id| {
            UnSubscribeCommand::new(correlation_id, subscription_id)
        })
        .await
    }

    pub async fn partitions(
        &self,
        super_stream: String,
    ) -> RabbitMQStreamResult<SuperStreamPartitionsResponse> {
        self.connection.send_and_receive(|correlation_id| {
            SuperStreamPartitionsRequest::new(correlation_id, super_stream)
        })
        .await
    }

    pub async fn route(
        &self,
        routing_key: String,
        super_stream: String,
    ) -> RabbitMQStreamResult<SuperStreamRouteResponse> {
        self.connection.send_and_receive(|correlation_id| {
            SuperStreamRouteRequest::new(correlation_id, routing_key, super_stream)
        })
        .await
    }

    pub async fn create_stream(
        &self,
        stream: &str,
        options: HashMap<String, String>,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.connection.send_and_receive(|correlation_id| {
            CreateStreamCommand::new(correlation_id, stream.to_owned(), options)
        })
        .await
    }

    pub async fn create_super_stream(
        &self,
        super_stream: &str,
        partitions: Vec<String>,
        binding_keys: Vec<String>,
        options: HashMap<String, String>,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.connection.send_and_receive(|correlation_id| {
            CreateSuperStreamCommand::new(
                correlation_id,
                super_stream.to_owned(),
                partitions,
                binding_keys,
                options,
            )
        })
        .await
    }

    pub async fn delete_stream(&self, stream: &str) -> RabbitMQStreamResult<GenericResponse> {
        self.connection.send_and_receive(|correlation_id| Delete::new(correlation_id, stream.to_owned()))
            .await
    }

    pub async fn delete_super_stream(
        &self,
        super_stream: &str,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.connection.send_and_receive(|correlation_id| {
            DeleteSuperStreamCommand::new(correlation_id, super_stream.to_owned())
        })
        .await
    }

    pub async fn credit(&self, subscription_id: u8, credit: u16) -> RabbitMQStreamResult<()> {
        self.connection.send(CreditCommand::new(subscription_id, credit)).await
    }

    pub async fn metadata(
        &self,
        streams: Vec<String>,
    ) -> RabbitMQStreamResult<HashMap<String, StreamMetadata>> {
        self.connection.send_and_receive(|correlation_id| MetadataCommand::new(correlation_id, streams))
            .await
            .map(metadata::from_response)
    }

    pub async fn store_offset(
        &self,
        reference: &str,
        stream: &str,
        offset: u64,
    ) -> RabbitMQStreamResult<()> {
        self.connection.send(StoreOffset::new(
            reference.to_owned(),
            stream.to_owned(),
            offset,
        ))
        .await
    }

    pub async fn query_offset(&self, reference: String, stream: &str) -> Result<u64, ClientError> {
        let response = self.connection
            .send_and_receive::<QueryOffsetResponse, _, _>(|correlation_id| {
                QueryOffsetRequest::new(correlation_id, reference, stream.to_owned())
            })
            .await?;

        if !response.is_ok() {
            Err(ClientError::RequestError(response.code().clone()))
        } else {
            Ok(response.from_response())
        }
    }

    pub async fn declare_publisher(
        &self,
        publisher_id: u8,
        publisher_reference: Option<String>,
        stream: &str,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.connection.send_and_receive(|correlation_id| {
            DeclarePublisherCommand::new(
                correlation_id,
                publisher_id,
                publisher_reference,
                stream.to_owned(),
            )
        })
        .await
    }

    pub async fn delete_publisher(
        &self,
        publisher_id: u8,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.connection.send_and_receive(|correlation_id| {
            DeletePublisherCommand::new(correlation_id, publisher_id)
        })
        .await
    }

    pub async fn publish<T: BaseMessage>(
        &self,
        publisher_id: u8,
        messages: impl Into<Vec<T>>,
        version: u16,
    ) -> RabbitMQStreamResult<Vec<u64>> {
        let messages: Vec<PublishedMessage> = messages
            .into()
            .into_iter()
            .map(|message| {
                let publishing_id: u64 = message
                    .publishing_id()
                    .unwrap_or_else(|| self.publish_sequence.fetch_add(1, Ordering::Relaxed));
                let filter_value = message.filter_value();
                PublishedMessage::new(publishing_id, message.to_message(), filter_value)
            })
            .collect();
        let sequences = messages
            .iter()
            .map(rabbitmq_stream_protocol::types::PublishedMessage::publishing_id)
            .collect();
        let len = messages.len();

        // TODO batch publish with max frame size check
        self.connection.send(PublishCommand::new(publisher_id, messages, version))
            .await?;

        self.opts.collector.publish(len as u64).await;

        Ok(sequences)
    }

    pub async fn query_publisher_sequence(
        &self,
        reference: &str,
        stream: &str,
    ) -> Result<u64, ClientError> {
        self.connection.send_and_receive::<QueryPublisherResponse, _, _>(|correlation_id| {
            QueryPublisherRequest::new(correlation_id, reference.to_owned(), stream.to_owned())
        })
        .await
        .map(|sequence| sequence.from_response())
    }

    pub async fn exchange_command_versions(
        &self,
    ) -> RabbitMQStreamResult<ExchangeCommandVersionsResponse> {
        self.connection.send_and_receive::<ExchangeCommandVersionsResponse, _, _>(|correlation_id| {
            ExchangeCommandVersionsRequest::new(correlation_id, vec![])
        })
        .await
    }

    pub fn filtering_supported(&self) -> bool {
        self.connection.filtering_supported()
    }

    async fn create_connection(
        broker: &ClientOptions,
    ) -> Result<
        (
            ChannelSender<SinkConnection>,
            ChannelReceiver<StreamConnection>,
        ),
        ClientError,
    > {
        let stream = broker.build_generic_tcp_stream().await?;
        let stream = Framed::new(stream, RabbitMqStreamCodec {});

        let (sink, stream) = stream.split();
        let (tx, rx) = channel(sink, stream);

        Ok((tx, rx))
    }

    pub async fn consumer_update(
        &self,
        correlation_id: u32,
        offset_specification: OffsetSpecification,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.connection.send_and_receive(|_| {
            ConsumerUpdateRequestCommand::new(correlation_id, 1, offset_specification)
        })
        .await
    }
}
