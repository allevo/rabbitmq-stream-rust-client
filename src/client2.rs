use dashmap::DashMap;
use rabbitmq_stream_protocol::{
    commands::{
        consumer_update_request::ConsumerUpdateRequestCommand, create_stream::CreateStreamCommand, create_super_stream::CreateSuperStreamCommand, credit::CreditCommand, declare_publisher::DeclarePublisherCommand, delete::Delete, delete_publisher::DeletePublisherCommand, delete_super_stream::DeleteSuperStreamCommand, exchange_command_versions::{ExchangeCommandVersionsRequest, ExchangeCommandVersionsResponse}, generic::GenericResponse, metadata::MetadataCommand, publish::PublishCommand, query_offset::{QueryOffsetRequest, QueryOffsetResponse}, query_publisher_sequence::{QueryPublisherRequest, QueryPublisherResponse}, store_offset::StoreOffset, subscribe::{OffsetSpecification, SubscribeCommand}, superstream_partitions::{SuperStreamPartitionsRequest, SuperStreamPartitionsResponse}, superstream_route::{SuperStreamRouteRequest, SuperStreamRouteResponse}, unsubscribe::UnSubscribeCommand
    },
    types::PublishedMessage,
    FromResponse, Request, Response,
};
use raw::RawClient;
use tracing::{error, trace};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::oneshot::{channel, Sender};

use crate::{
    client::message::BaseMessage, error::ClientError, ClientOptions, RabbitMQStreamResult,
};

#[derive(Debug)]
pub enum CloseReason {
    ConnectionClosed,
    User,
    Error(ClientError),
}

#[async_trait::async_trait]
pub trait ResponseHandler: Send + Sync + 'static {
    async fn handle_response(&self, item: Response);
    async fn on_closed(&self, reason: CloseReason);
}

pub struct Client2 {
    raw_client: Arc<RawClient>,
    correlation_id: AtomicU32,
    publish_sequence: AtomicU64,
    requests: Arc<DashMap<u32, Sender<Response>>>,
}

struct ResponseHandlerWrapper(
    Option<Box<dyn ResponseHandler>>,
    Arc<RawClient>,
    Arc<DashMap<u32, Sender<Response>>>,
);
#[async_trait::async_trait]
impl ResponseHandler for ResponseHandlerWrapper {
    async fn handle_response(&self, response: Response) {
        if let Some(correlation_id) = response.correlation_id() {
            if let Some((_, sender)) = self.2.remove(&correlation_id) {
                sender
                    .send(response)
                    .unwrap_or_else(|e| {
                        error!("Failed to send response to channel: {:?}", e);
                    });
                return;
            }
        }

        if let Some(handler) = self.0.as_ref() {
            handler.handle_response(response).await;
        }
    }

    async fn on_closed(&self, reason: CloseReason) {
        if let Some(handler) = self.0.as_ref() {
            handler.on_closed(reason).await;
        }
    }
}

impl Client2 {
    pub async fn connect(opts: impl Into<ClientOptions>) -> Result<Client2, ClientError> {
        let raw_client = RawClient::connect(opts).await?;
        let raw_client = Arc::new(raw_client);

        let requests = Arc::new(DashMap::new());

        let handler = Box::new(ResponseHandlerWrapper(None, raw_client.clone(), requests.clone()));
        let mut lock = raw_client.state.write().await;
        lock.response_handler = Some(handler);
        drop(lock);

        Ok(Client2 {
            raw_client,
            correlation_id: AtomicU32::new(0),
            publish_sequence: AtomicU64::new(0),
            requests,
        })
    }

    pub async fn set_handler(&self, handler: Box<dyn ResponseHandler>) {
        let handler = Box::new(ResponseHandlerWrapper(Some(handler), self.raw_client.clone(), self.requests.clone()));

        let mut lock = self.raw_client.state.write().await;
        lock.response_handler = Some(handler);
    }

    /// Get client's server properties.
    pub async fn server_properties(&self) -> HashMap<String, String> {
        self.raw_client.connection_data.server_properties.clone()
    }

    /// Get client's connection properties.
    pub async fn connection_properties(&self) -> HashMap<String, String> {
        self.raw_client
            .connection_data
            .connection_properties
            .clone()
    }

    pub async fn is_closed(&self) -> bool {
        let lock = self.raw_client.state.read().await;
        let close_reason = lock.close_reason.read().await;
        close_reason.is_some()
    }

    pub async fn close(&self) -> RabbitMQStreamResult<()> {
        self.raw_client.close(CloseReason::User).await
    }

    pub async fn subscribe(
        &self,
        subscription_id: u8,
        stream: &str,
        offset_specification: OffsetSpecification,
        credit: u16,
        properties: HashMap<String, String>,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
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
        self.send_and_receive(|correlation_id| {
            UnSubscribeCommand::new(correlation_id, subscription_id)
        })
        .await
    }

    pub async fn partitions(
        &self,
        super_stream: String,
    ) -> RabbitMQStreamResult<SuperStreamPartitionsResponse> {
        self.send_and_receive(|correlation_id| {
            SuperStreamPartitionsRequest::new(correlation_id, super_stream)
        })
        .await
    }

    pub async fn route(
        &self,
        routing_key: String,
        super_stream: String,
    ) -> RabbitMQStreamResult<SuperStreamRouteResponse> {
        self.send_and_receive(|correlation_id| {
            SuperStreamRouteRequest::new(correlation_id, routing_key, super_stream)
        })
        .await
    }

    pub async fn create_stream(
        &self,
        stream: &str,
        options: HashMap<String, String>,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
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
        self.send_and_receive(|correlation_id| {
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
        self.send_and_receive(|correlation_id| Delete::new(correlation_id, stream.to_owned()))
            .await
    }

    pub async fn delete_super_stream(
        &self,
        super_stream: &str,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|correlation_id| {
            DeleteSuperStreamCommand::new(correlation_id, super_stream.to_owned())
        })
        .await
    }

    pub async fn credit(&self, subscription_id: u8, credit: u16) -> RabbitMQStreamResult<()> {
        self.send(CreditCommand::new(subscription_id, credit)).await
    }

    pub async fn metadata(
        &self,
        streams: Vec<String>,
    ) -> RabbitMQStreamResult<HashMap<String, crate::client::metadata::StreamMetadata>> {
        self.send_and_receive(|correlation_id| MetadataCommand::new(correlation_id, streams))
            .await
            .map(crate::client::metadata::from_response)
    }

    pub async fn store_offset(
        &self,
        reference: &str,
        stream: &str,
        offset: u64,
    ) -> RabbitMQStreamResult<()> {
        self.send(StoreOffset::new(
            reference.to_owned(),
            stream.to_owned(),
            offset,
        ))
        .await
    }

    pub async fn query_offset(&self, reference: String, stream: &str) -> Result<u64, ClientError> {
        let response = self
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
        self.send_and_receive(|correlation_id| {
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
        self.send_and_receive(|correlation_id| {
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
        self.send(PublishCommand::new(publisher_id, messages, version))
            .await?;

        self.raw_client
            .connection_data
            .client_options
            .collector
            .publish(len as u64)
            .await;

        Ok(sequences)
    }

    pub async fn query_publisher_sequence(
        &self,
        reference: &str,
        stream: &str,
    ) -> Result<u64, ClientError> {
        self.send_and_receive::<QueryPublisherResponse, _, _>(|correlation_id| {
            QueryPublisherRequest::new(correlation_id, reference.to_owned(), stream.to_owned())
        })
        .await
        .map(|sequence| sequence.from_response())
    }

    pub async fn consumer_update(
        &self,
        correlation_id: u32,
        offset_specification: OffsetSpecification,
    ) -> RabbitMQStreamResult<GenericResponse> {
        self.send_and_receive(|_| {
            // warn!("--- Sending consumer update {:?}", offset_specification);
            ConsumerUpdateRequestCommand::new(correlation_id, 1, offset_specification)
        })
        .await
    }

    pub async fn exchange_command_versions(
        &self,
    ) -> RabbitMQStreamResult<ExchangeCommandVersionsResponse> {
        self.send_and_receive::<ExchangeCommandVersionsResponse, _, _>(|correlation_id| {
            ExchangeCommandVersionsRequest::new(correlation_id, vec![])
        })
        .await
    }

    pub fn filtering_supported(&self) -> bool {
        self.raw_client.connection_data.filtering_supported
    }

    async fn send_and_receive<Res, Req, M>(&self, msg_factory: M) -> Result<Res, ClientError>
    where
        Req: Into<Request>,
        Res: FromResponse,
        M: FnOnce(u32) -> Req,
    {
        let correlation_id = self
            .correlation_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = channel();
        self.requests.insert(correlation_id, tx);

        let request = msg_factory(correlation_id).into();

        trace!("corr_di {}. Sending request  {:?}", correlation_id, request);
        self.send(request).await?;
        trace!("Waiting for response");

        let response = rx.await.map_err(|_| ClientError::ConnectionClosed)?;

        Self::handle_response::<Res>(response).await
    }

    #[inline]
    async fn send<R>(&self, msg: R) -> Result<(), ClientError>
    where
        R: Into<Request>,
    {
        self.raw_client.send_request(msg.into()).await
    }

    async fn handle_response<Res: FromResponse>(response: Response) -> Result<Res, ClientError> {
        response.get::<Res>().ok_or_else(|| {
            ClientError::CastError(format!(
                "Cannot cast response to {}",
                std::any::type_name::<Res>()
            ))
        })
    }
}

mod raw {
    use crate::{client2::CloseReason, error::ClientError, ClientOptions, RabbitMQStreamResult};

    use super::{
        connection::{create_connection, initialize, ConnectionData, RequestSender},
        ResponseHandler,
    };
    use rabbitmq_stream_protocol::{commands::heart_beat::HeartBeatCommand, Request, ResponseKind};
    use std::{
        sync::{atomic::{AtomicBool, AtomicUsize}, Arc},
        time::{Duration, Instant},
    };
    use tokio::sync::RwLock;
    use tracing::{trace, warn};

    /// This structure holds the state of the client connection
    /// It is shared between thread:
    /// - application thread to send requests
    /// - background thread to handle responses
    /// - background thread to send heartbeats
    /// Nested `RwLock` are used to avoid waiting on the lock
    /// when sending requests or receiving heartbeats
    pub struct ShareClientRawState {
        pub response_handler: Option<Box<dyn ResponseHandler>>,
        last_heatbeat: RwLock<Instant>,
        request_sender: Arc<RwLock<RequestSender>>,
        pub close_reason: RwLock<Option<CloseReason>>,
    }

    impl ShareClientRawState {
        pub async fn close(&self, reason: CloseReason) -> RabbitMQStreamResult<()> {
            // This guarantees we perform the closing process only once
            let close_reason_lock = &mut *self.close_reason.write().await;
            match close_reason_lock {
                None => {
                    *close_reason_lock = Some(reason);
                },
                Some(_) => {
                    return RabbitMQStreamResult::Err(ClientError::ConnectionClosed);
                }
            };

            
            let mut s = self.request_sender.write().await;
            s.close().await?;

            Ok(())
        }
    }

    pub struct RawClient {
        pub connection_data: ConnectionData,
        pub state: Arc<RwLock<ShareClientRawState>>,
        response_loop_join_handler: tokio::task::JoinHandle<()>,
        heartbeat_task: Option<tokio::task::JoinHandle<()>>,
        id: usize,
    }

    static RAW_CLIENT_ID: AtomicUsize = AtomicUsize::new(0);

    impl RawClient {
        pub async fn connect(opts: impl Into<ClientOptions>) -> Result<RawClient, ClientError> {
            let id = RAW_CLIENT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let client_options = opts.into();

            let mut pair = create_connection(&client_options).await?;

            let connection_data = initialize(&client_options, &mut pair).await?;
            let heartbeat = connection_data.heartbeat;

            let (request_sender, mut response_receiver) = pair.split();
            let request_sender = Arc::new(RwLock::new(request_sender));

            let state = Arc::new(RwLock::new(ShareClientRawState {
                response_handler: None,
                // Even if the heartbeat is not set yet
                // we consider the first heartbeat as the start of the connection
                last_heatbeat: RwLock::new(Instant::now()),
                request_sender,
                close_reason: RwLock::new(None),
            }));

            let receive_loop_state = state.clone();
            let response_loop_join_handler = tokio::spawn(async move {
                // TODO implements Error handling and close of dispatcher
                loop {
                    warn!("Waiting for response {:?}", id);
                    let Some(result) = response_receiver.receive().await else {
                        warn!("Response receiver is closed. Stopping receive loop");
                        break;
                    };
                    trace!("Received message: {:?}", result);
                    match result {
                        Ok(response) => {
                            let lock = receive_loop_state.read().await;
                            if let Some(handler) = lock.response_handler.as_ref() {
                                if matches!(response.kind_ref(), ResponseKind::Heartbeat(_)) {
                                    trace!("Received heartbeat response {:?}", id);
                                    let mut last_heatbeat_lock = lock.last_heatbeat.write().await;
                                    *last_heatbeat_lock = Instant::now();
                                } else {
                                    handler.handle_response(response).await;
                                }
                            }
                            drop(lock);
                        }
                        Err(e) => {
                            warn!("Error from stream {:?}", e);
                            break;
                        }
                    }
                }
                warn!("Receive loop stopped. Force closing connection {:?}", id);
                let state_lock = receive_loop_state.read().await;
                state_lock.close(CloseReason::ConnectionClosed).await.unwrap();
            });

            let mut heartbeat_task = None;
            if heartbeat > 0 {
                let state = state.clone();
                let heartbeat_interval = (heartbeat / 2).max(1);
                let task = tokio::spawn(async move {
                    let mut now = tokio::time::Instant::now();
                    // Skip the first heartbeat
                    now += Duration::from_secs(heartbeat_interval.into());
                    let mut interval = tokio::time::interval_at(
                        now,
                        Duration::from_secs(heartbeat_interval.into()),
                    );
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
                    loop {
                        interval.tick().await;
                        trace!("Sending heartbeat id {:?}", id);
                        let state_lock = state.read().await;
                        trace!("Read acquired. Try to acquire write lock{:?}", id);
                        let mut request_sender_lock = state_lock.request_sender.write().await;
                        trace!("Write acquired.{:?}", id);
                        if request_sender_lock.is_closed() {
                            warn!("Request sender is closed. Stopping heartbeat task");
                            break;
                        }

                        if let Err(e) = request_sender_lock
                            .send(HeartBeatCommand::default().into())
                            .await
                        {
                            warn!("Error sending heartbeat: {:?}", e);
                            break;
                        }
                        drop(request_sender_lock);
                        drop(state_lock);
                    }

                    warn!("Heartbeat task stopped. Force closing connection {:?}", id);
                });
                heartbeat_task = Some(task);
            }

            Ok(RawClient {
                connection_data,
                state,
                response_loop_join_handler,
                heartbeat_task,
                id,
            })
        }

        pub async fn send_request(&self, request: Request) -> Result<(), ClientError> {
            let lock = self.state.read().await;
            let mut request_sender_lock = lock.request_sender.write().await;
            request_sender_lock.send(request).await?;
            Ok(())
        }

        pub async fn close(&self, reason: CloseReason) -> RabbitMQStreamResult<()> {
            let state_lock = self.state.read().await;
            state_lock.close(reason).await?;

            trace!("Stopping heartbeat task");
            if let Some(heartbeat_task) = self.heartbeat_task.as_ref() {
                heartbeat_task.abort();
            }
            trace!("Stopping response loop task");
            self.response_loop_join_handler.abort();


            Ok(())
        }
    }

    #[cfg(test)]
    mod test {
        use rabbitmq_stream_protocol::commands::credit::CreditCommand;
        use tokio::time::sleep;

        use super::*;
        use crate::client::ClientOptions;

        #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
        async fn test_raw_client() {
            let _ = tracing_subscriber::fmt::try_init();

            let client_options = ClientOptions::default();
            let raw_client = RawClient::connect(client_options.clone()).await.unwrap();

            raw_client.close(CloseReason::User).await.unwrap();

            // This is needed because `response_loop_join_handler` is finishing but not yet finished.
            sleep(Duration::from_millis(100)).await;

            assert!(raw_client.response_loop_join_handler.is_finished());
            // Heartbeat task is not finished because it is not aborted
            // assert!(raw_client.heartbeat_task.as_ref().unwrap().is_finished());

            let command = CreditCommand::new(1, 1);
            let res = raw_client.send_request(command.into()).await;
            assert!(matches!(res, Err(ClientError::ConnectionClosed)));
        }
    }
}

mod connection {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::client::GenericTcpStream;
    use crate::RabbitMQStreamResult;
    use crate::{error::ClientError, ClientOptions};
    use bytes::{Buf, BufMut, BytesMut};
    use futures::stream::{SplitSink, SplitStream};
    use futures::{SinkExt, StreamExt};

    use rabbitmq_stream_protocol::codec::{Decoder, Encoder};
    use rabbitmq_stream_protocol::commands::exchange_command_versions::{
        ExchangeCommandVersionsRequest, ExchangeCommandVersionsResponse,
    };
    use rabbitmq_stream_protocol::commands::generic::GenericResponse;
    use rabbitmq_stream_protocol::commands::open::{OpenCommand, OpenResponse};
    use rabbitmq_stream_protocol::commands::peer_properties::{
        PeerPropertiesCommand, PeerPropertiesResponse,
    };
    use rabbitmq_stream_protocol::commands::sasl_authenticate::SaslAuthenticateCommand;
    use rabbitmq_stream_protocol::commands::sasl_handshake::{
        SaslHandshakeCommand, SaslHandshakeResponse,
    };
    use rabbitmq_stream_protocol::commands::tune::TunesCommand;
    use rabbitmq_stream_protocol::error::DecodeError;
    use rabbitmq_stream_protocol::{FromResponse, Request, Response};

    use tokio_util::codec::Framed;
    use tokio_util::codec::{Decoder as TokioDecoder, Encoder as TokioEncoder};
    use tracing::{trace, warn};

    impl TokioDecoder for RabbitMqStreamCodec {
        type Item = Response;
        type Error = ClientError;

        fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Response>, ClientError> {
            println!("---- buff: {:?}", buf);
            match Response::decode(buf) {
                Ok((remaining, response)) => {
                    let len = remaining.len();
                    buf.advance(buf.len() - len);
                    Ok(Some(response))
                }
                Err(DecodeError::Incomplete(_)) => Ok(None),
                Err(e) => Err(e.into()),
            }
        }
    }

    impl TokioEncoder<Request> for RabbitMqStreamCodec {
        type Error = ClientError;

        fn encode(&mut self, req: Request, buf: &mut BytesMut) -> Result<(), ClientError> {
            let len = req.encoded_size();
            buf.reserve(len as usize);
            let mut writer = buf.writer();
            req.encode(&mut writer)?;

            Ok(())
        }
    }

    type SinkConnection = SplitSink<Framed<GenericTcpStream, RabbitMqStreamCodec>, Request>;
    type StreamConnection = SplitStream<Framed<GenericTcpStream, RabbitMqStreamCodec>>;

    #[derive(Debug)]
    struct RabbitMqStreamCodec;

    pub async fn create_connection(client_options: &ClientOptions) -> Result<Pair, ClientError> {
        let stream = client_options.build_generic_tcp_stream().await?;
        let stream = Framed::new(stream, RabbitMqStreamCodec);

        let (sink, stream): (SinkConnection, StreamConnection) = stream.split();

        let request_sender = RequestSender { sink, is_closed: AtomicBool::new(false) };
        let response_receiver = ResponseReceiver { stream };

        Ok(Pair {
            request_sender,
            response_receiver,
        })
    }

    pub struct Pair {
        pub request_sender: RequestSender,
        pub response_receiver: ResponseReceiver,
    }

    impl Pair {
        pub async fn send_and_receive<Req, Res>(&mut self, request: Req) -> Result<Res, ClientError>
        where
            Req: Into<Request>,
            Res: FromResponse,
        {
            self.request_sender.send(request.into()).await?;
            let response = match self.response_receiver.receive().await {
                Some(Ok(item)) => item,
                Some(Err(e)) => return Err(e),
                None => return Err(ClientError::ConnectionClosed),
            };

            response.get::<Res>().ok_or_else(|| {
                ClientError::CastError(format!(
                    "Cannot cast response to {}",
                    std::any::type_name::<Res>()
                ))
            })
        }

        pub fn split(self) -> (RequestSender, ResponseReceiver) {
            let Pair {
                request_sender,
                response_receiver,
            } = self;
            (request_sender, response_receiver)
        }
    }

    pub struct RequestSender {
        sink: SinkConnection,
        is_closed: AtomicBool,
    }

    impl RequestSender {
        pub async fn send(&mut self, item: Request) -> RabbitMQStreamResult<()> {
            if self.is_closed() {
                return RabbitMQStreamResult::Err(ClientError::ConnectionClosed);
            }
            self.sink.send(item).await
        }

        pub async fn close(&mut self) -> RabbitMQStreamResult<()> {
            let is_closed = self.is_closed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
            if is_closed.is_err() {
                return RabbitMQStreamResult::Err(ClientError::ConnectionClosed);
            }

            self.sink.close().await
        }

        pub fn is_closed(&self) -> bool {
            self.is_closed.load(Ordering::SeqCst)
        }
    }

    pub struct ResponseReceiver {
        stream: StreamConnection,
    }

    impl ResponseReceiver {
        pub async fn receive(&mut self) -> Option<Result<Response, ClientError>> {
            let r = self.stream.next().await;
            // warn!("Received response: {:?}", r);
            r
        }
    }

    pub async fn initialize(
        client_options: &ClientOptions,
        pair: &mut Pair,
    ) -> Result<ConnectionData, ClientError> {
        const VERSION: &str = env!("CARGO_PKG_VERSION");

        let mut client_properties = HashMap::new();
        client_properties.insert(String::from("product"), String::from("RabbitMQ"));
        client_properties.insert(String::from("version"), String::from(VERSION));
        client_properties.insert(String::from("platform"), String::from("Rust"));
        client_properties.insert(
            String::from("copyright"),
            String::from("Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries."));
        client_properties.insert(
            String::from("information"),
            String::from(
                "Licensed under the Apache 2.0 and MPL 2.0 licenses. See https://www.rabbitmq.com/",
            ),
        );
        client_properties.insert(
            String::from("connection_name"),
            client_options.client_provided_name.clone(),
        );

        let mut correlation_id = 0;

        correlation_id += 1;
        let command = PeerPropertiesCommand::new(correlation_id, client_properties.clone());
        let response: PeerPropertiesResponse = pair.send_and_receive(command).await?;
        let server_properties = response.server_properties;

        correlation_id += 1;
        let command = SaslHandshakeCommand::new(correlation_id);
        let response: SaslHandshakeResponse = pair.send_and_receive(command).await?;
        let handshake_mechanisms = response.mechanisms;

        correlation_id += 1;
        let auth_data = format!(
            "\u{0000}{}\u{0000}{}",
            client_options.user, client_options.password
        );
        let command = SaslAuthenticateCommand::new(
            correlation_id,
            "PLAIN".to_owned(),
            auth_data.as_bytes().to_vec(),
        );
        let response: GenericResponse = pair.send_and_receive(command).await?;
        if !response.is_ok() {
            return Err(ClientError::RequestError(response.code().clone()));
        }

        let response = match pair.response_receiver.receive().await {
            None => return Err(ClientError::ConnectionClosed),
            Some(Ok(item)) => item,
            Some(Err(e)) => return Err(e),
        };
        let response: TunesCommand = response.get().ok_or_else(|| {
            ClientError::CastError(format!(
                "Cannot cast response to {}",
                std::any::type_name::<TunesCommand>()
            ))
        })?;
        let heartbeat = response.heartbeat;
        let max_frame_size = response.max_frame_size;
        trace!(
            "Handling tune with frame size {} and heartbeat {}",
            max_frame_size,
            heartbeat
        );
        pair.request_sender
            .send(TunesCommand::new(max_frame_size, heartbeat).into())
            .await?;

        correlation_id += 1;
        let command = OpenCommand::new(correlation_id, client_options.v_host.clone());
        let response: OpenResponse = pair.send_and_receive(command).await?;
        if !response.is_ok() {
            return Err(ClientError::RequestError(response.code().clone()));
        }
        let connection_properties = response.connection_properties;

        correlation_id += 1;
        let command = ExchangeCommandVersionsRequest::new(correlation_id, vec![]);
        let response: ExchangeCommandVersionsResponse = pair.send_and_receive(command).await?;
        let (_, max_version) = response.key_version(2);
        let filtering_supported = max_version >= 2;

        Ok(ConnectionData {
            client_options: client_options.clone(),
            client_properties,
            server_properties,
            connection_properties,
            filtering_supported,
            heartbeat,
            max_frame_size,
            handshake_mechanisms,
        })
    }

    pub struct ConnectionData {
        pub client_options: ClientOptions,
        pub client_properties: HashMap<String, String>,
        pub server_properties: HashMap<String, String>,
        pub connection_properties: HashMap<String, String>,
        pub filtering_supported: bool,
        pub heartbeat: u32,
        pub max_frame_size: u32,
        pub handshake_mechanisms: Vec<String>,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::client::ClientOptions;

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_client_close() {
        let _ = tracing_subscriber::fmt::try_init();

        let client_options = ClientOptions::default();
        let client = Client2::connect(client_options.clone()).await.unwrap();

        client.close().await.unwrap();
        // sleep(Duration::from_millis(100)).await;

        let res = client.credit(1, 1).await;
        assert!(matches!(res, Err(ClientError::ConnectionClosed)));
    }
}