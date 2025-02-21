use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use rabbitmq_stream_protocol::{
    commands::{
        close::{CloseRequest, CloseResponse},
        exchange_command_versions::{
            ExchangeCommandVersionsRequest, ExchangeCommandVersionsResponse,
        },
        generic::GenericResponse,
        heart_beat::HeartBeatCommand,
        open::{OpenCommand, OpenResponse},
        peer_properties::{PeerPropertiesCommand, PeerPropertiesResponse},
        sasl_authenticate::SaslAuthenticateCommand,
        sasl_handshake::{SaslHandshakeCommand, SaslHandshakeResponse},
        tune::TunesCommand,
    },
    FromResponse, Request, Response, ResponseCode,
};
use tokio::{
    sync::Mutex,
    time::{interval_at, MissedTickBehavior},
};
use tokio_util::codec::Framed;
use tracing::{error, info, instrument, trace, warn};

use crate::{error::ClientError, RabbitMQStreamResult};

use super::{codec::RabbitMqStreamCodec, ClientOptions, GenericTcpStream, MessageHandler};

#[async_trait::async_trait]
pub trait CloseCallback: Send + Sync + 'static {
    async fn on_close(self: Box<Self>);
}

#[async_trait::async_trait]
impl<T, F> CloseCallback for T
where
    F: Future<Output = ()> + Send,
    T: FnOnce() -> F + Send + Sync,
    T: 'static,
{
    async fn on_close(self: Box<Self>) {
        self().await
    }
}

#[async_trait::async_trait]
pub trait HandleResponseMessage: Send + Sync {
    async fn on_response(&self, response: Response);
}
#[async_trait::async_trait]
impl<T, F> HandleResponseMessage for T
where
    F: Future<Output = ()> + Send,
    T: Fn(Response) -> F + Send + Sync,
    T: Clone,
{
    async fn on_response(&self, response: Response) {
        let f = self.clone();
        (f)(response).await
    }
}

use futures::future::BoxFuture;

pub struct Connection {
    // Connection properties
    client_properties: HashMap<String, String>,
    server_properties: HashMap<String, String>,
    filtering_supported: bool,
    connection_properties: HashMap<String, String>,

    // Utilities to wait the reply
    correlation_id: AtomicU32,
    correlation_map: Mutex<HashMap<u32, tokio::sync::oneshot::Sender<Response>>>,

    is_connected: AtomicBool,
    stop_heartbeat_sender: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    sender: Arc<Mutex<SplitSink<Framed<GenericTcpStream, RabbitMqStreamCodec>, Request>>>,
    close_callback: Mutex<Option<Box<dyn CloseCallback>>>,
    handle_response_message: Mutex<Option<Arc<dyn MessageHandler>>>,
}

impl Connection {
    pub async fn connect(
        client_options: impl Into<ClientOptions>,
    ) -> RabbitMQStreamResult<Arc<Self>> {
        let client_options = client_options.into();
        let stream = client_options.build_generic_tcp_stream().await?;
        let stream = Framed::new(stream, RabbitMqStreamCodec {});
        let (mut sender, mut receiver) = stream.split();

        let client_properties = get_client_properties(&client_options);

        let mut correlation_id = 0;
        let mut generate_collelation_id = || {
            correlation_id += 1;
            correlation_id
        };

        // get server_properties
        let request =
            PeerPropertiesCommand::new(generate_collelation_id(), client_properties.clone());
        let response: PeerPropertiesResponse =
            send_and_receive(&mut sender, &mut receiver, request).await?;
        let server_properties = response.server_properties;

        // Check SaslAuth support
        let request = SaslHandshakeCommand::new(generate_collelation_id());
        let response: SaslHandshakeResponse =
            send_and_receive(&mut sender, &mut receiver, request).await?;
        if !response.mechanisms.contains(&"PLAIN".to_string()) {
            return Err(ClientError::UnsupportedAuthMechanism(
                "PLAIN".to_string(),
                response.mechanisms,
            ));
        }

        // Auth
        let auth_data = format!(
            "\u{0000}{}\u{0000}{}",
            client_options.user, client_options.password
        );
        let request = SaslAuthenticateCommand::new(
            generate_collelation_id(),
            "PLAIN".to_owned(),
            auth_data.as_bytes().to_vec(),
        );
        let response: GenericResponse =
            send_and_receive(&mut sender, &mut receiver, request).await?;
        if !response.is_ok() {
            return Err(ClientError::RequestError(response.code().clone()));
        }

        let tunes: TunesCommand = receive(&mut receiver).await?;
        let heartbeat = negotiate_value(client_options.heartbeat, tunes.heartbeat);
        let max_frame_size = negotiate_value(client_options.max_frame_size, tunes.max_frame_size);

        trace!(
            "Handling tune with frame size {} and heartbeat {}",
            max_frame_size,
            heartbeat
        );

        let request: TunesCommand = TunesCommand::new(max_frame_size, heartbeat).into();
        send(&mut sender, request).await?;

        // Connect to v_host
        let request = OpenCommand::new(generate_collelation_id(), client_options.v_host.clone());
        let response: OpenResponse = send_and_receive(&mut sender, &mut receiver, request).await?;
        if !response.is_ok() {
            return Err(ClientError::RequestError(response.code().clone()));
        }
        let connection_properties = response.connection_properties;

        // Version

        let request = ExchangeCommandVersionsRequest::new(generate_collelation_id(), vec![]);
        let response: ExchangeCommandVersionsResponse =
            send_and_receive(&mut sender, &mut receiver, request).await?;
        let (_, max_version) = response.key_version(2);
        let filtering_supported = max_version >= 2;

        let sender = Arc::new(Mutex::new(sender));

        // Hearthbeat
        let (stop_heartbeat_sender, stop_heartbeat_receiver) = tokio::sync::oneshot::channel();
        start_heartbeat(sender.clone(), heartbeat, stop_heartbeat_receiver);

        let client = Connection {
            is_connected: AtomicBool::new(true),
            stop_heartbeat_sender: Mutex::new(Some(stop_heartbeat_sender)),
            client_properties,
            server_properties,
            connection_properties,
            filtering_supported,
            correlation_id: AtomicU32::new(correlation_id),
            sender,
            correlation_map: Mutex::new(HashMap::new()),
            close_callback: Mutex::new(None),
            handle_response_message: Mutex::new(None),
        };
        let client = Arc::new(client);

        handle_response(receiver, client.clone());

        Ok(client)
    }

    pub fn connection_properties(&self) -> &HashMap<String, String> {
        &self.connection_properties
    }

    pub fn server_properties(&self) -> &HashMap<String, String> {
        &self.server_properties
    }

    pub fn filtering_supported(&self) -> bool {
        self.filtering_supported
    }

    pub async fn set_handle_response_message(&self, handler: Arc<dyn MessageHandler>) {
        let mut handle_response_message = self.handle_response_message.lock().await;
        *handle_response_message = Some(handler);
    }

    pub async fn set_close_callback(&self, callback: Box<dyn CloseCallback>) {
        let mut close_callback = self.close_callback.lock().await;
        *close_callback = Some(callback);
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    pub async fn send<Req: Into<Request>>(&self, request: Req) -> RabbitMQStreamResult<()> {
        let mut sender = self.sender.lock().await;
        send(&mut sender, request).await?;
        drop(sender);

        Ok(())
    }

    pub async fn send_and_receive<Resp, Req, MsgFactory>(
        &self,
        msg_factory: MsgFactory,
    ) -> RabbitMQStreamResult<Resp>
    where
        Req: Into<Request>,
        Resp: FromResponse,
        MsgFactory: FnOnce(u32) -> Req,
    {
        let correlation_id = self.generate_collelation_id();
        let request = msg_factory(correlation_id);

        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut correlation_map = self.correlation_map.lock().await;
        correlation_map.insert(correlation_id, tx);
        drop(correlation_map);

        let mut sender = self.sender.lock().await;
        send(&mut sender, request).await?;
        drop(sender);

        let response = rx
            .await
            .map_err(|e| ClientError::GenericError(Box::new(e)))?;

        response.get::<Resp>().ok_or_else(|| {
            ClientError::CastError(format!(
                "Cannot cast response to {}.",
                std::any::type_name::<Resp>(),
            ))
        })
    }

    pub async fn close(&self) -> RabbitMQStreamResult<()> {
        let is_done =
            self.is_connected
                .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed);
        if is_done.is_err() {
            warn!("Double close. Ignore");
            return Ok(());
        }

        let mut close_callback = self.close_callback.lock().await;
        if let Some(close_callback) = close_callback.take() {
            close_callback.on_close().await;
        }

        let _: CloseResponse = self
            .send_and_receive(|correlation_id| {
                CloseRequest::new(correlation_id, ResponseCode::Ok, "Ok".to_owned())
            })
            .await?;

        let mut stop_heartbeat_sender = self.stop_heartbeat_sender.lock().await;
        let stop_heartbeat_sender = stop_heartbeat_sender.take().unwrap();
        if stop_heartbeat_sender.send(()).is_err() {
            warn!("Cannot stop heartbeat");
        }

        let mut sender = self.sender.lock().await;
        // This close closes the connection
        // That means all the tasks that are spawned to listen for messages
        // will be closed as well
        sender.close().await?;

        Ok(())
    }

    fn generate_collelation_id(&self) -> u32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn handle_response(&self, response: Response) {
        println!("handle_response {:?}", response);
        // Messages with correlation id is handled internally
        // They are comes from `send_and_receive` method
        if let Some(correlation_id) = response.correlation_id() {
            let mut correlation_map = self.correlation_map.lock().await;
            if let Some(sender) = correlation_map.remove(&correlation_id) {
                if let Err(err) = sender.send(response) {
                    warn!("Error sending response: {:?}", err);
                }
            }
            return;
        }
        let handle_response_message = self.handle_response_message.lock().await;
        if let Some(handle_response_message) = handle_response_message.as_ref() {
            let handler = handle_response_message.clone();
            if let Err(e) = handler.handle_message(Some(Ok(response))).await {
                error!("Error handling response: {:?}", e);
            }
        }
    }
}

fn negotiate_value(client: u32, server: u32) -> u32 {
    match (client, server) {
        (client, server) if client == 0 || server == 0 => client.max(server),
        (client, server) => client.min(server),
    }
}

fn get_client_properties(client_options: &ClientOptions) -> HashMap<String, String> {
    let mut client_properties = HashMap::new();
    const VERSION: &str = env!("CARGO_PKG_VERSION");

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

    client_properties
}

async fn send<Req: Into<Request>>(
    sender: &mut SplitSink<Framed<GenericTcpStream, RabbitMqStreamCodec>, Request>,
    request: Req,
) -> Result<(), ClientError> {
    let request = request.into();
    println!("send {:?}", request);
    sender.send(request).await
}
async fn receive<Resp: FromResponse>(
    receiver: &mut SplitStream<Framed<GenericTcpStream, RabbitMqStreamCodec>>,
) -> Result<Resp, ClientError> {
    let response = receiver
        .next()
        .await
        .ok_or(ClientError::ConnectionClosed)??;
    let kind = response.kind_ref();

    response.get::<Resp>().ok_or_else(|| {
        ClientError::CastError(format!(
            "Cannot cast response to {}.",
            std::any::type_name::<Resp>(),
        ))
    })
}
async fn send_and_receive<Req: Into<Request>, Resp: FromResponse>(
    sender: &mut SplitSink<Framed<GenericTcpStream, RabbitMqStreamCodec>, Request>,
    receiver: &mut SplitStream<Framed<GenericTcpStream, RabbitMqStreamCodec>>,
    request: Req,
) -> Result<Resp, ClientError> {
    send(sender, request).await?;
    receive(receiver).await
}

fn start_heartbeat(
    sender: Arc<Mutex<SplitSink<Framed<GenericTcpStream, RabbitMqStreamCodec>, Request>>>,
    heartbeat: u32,
    mut stop_heartbeat_receiver: tokio::sync::oneshot::Receiver<()>,
) {
    tokio::spawn(async move {
        let period = Duration::from_secs(heartbeat.into());
        let start = tokio::time::Instant::now() + period;
        let mut interval = interval_at(start, period);
        // Skip is the right behavior?
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    trace!("Sending heartbeat");
                }
                _ = &mut stop_heartbeat_receiver => {
                    trace!("Stop heartbeat");
                    break;
                }
            };
            let mut sender = sender.lock().await;

            match send(&mut sender, HeartBeatCommand::default()).await {
                Ok(_) => (),
                Err(e) => {
                    // Ignoring error is not the best way to handle it
                    // TODO: Implement error handling
                    warn!("Error sending heartbeat: {}", e);
                }
            }
        }
    });
}

#[instrument(skip(receiver, client))]
fn handle_response(
    mut receiver: SplitStream<Framed<GenericTcpStream, RabbitMqStreamCodec>>,
    client: Arc<Connection>,
) {
    tokio::spawn(async move {
        while let Some(result) = receiver.next().await {
            match result {
                Ok(response) => {
                    client.handle_response(response).await;
                }
                Err(e) => {
                    error!("Received error from stream {}", e);
                    break;
                }
            }
        }

        info!("Closing connection");
        let _ = client.close().await;
    });
}

#[cfg(test)]
mod tests {
    use fake::{Fake, Faker};
    use futures::FutureExt;
    use rabbitmq_stream_protocol::{
        commands::{
            create_stream::CreateStreamCommand,
            declare_publisher::DeclarePublisherCommand,
            metadata::MetadataResponse,
            publish::PublishCommand,
            subscribe::{OffsetSpecification, SubscribeCommand},
        },
        message::Message,
        types::PublishedMessage,
        ResponseKind,
    };
    use serde::{Deserialize, Serialize};
    use tokio::{sync::oneshot::channel, time::sleep};

    use crate::{
        stream_creator::LeaderLocator,
        types::{Broker, MessageResult, StreamMetadata},
    };

    use super::*;

    #[tokio::test]
    async fn test_new_client_close() {
        let _ = tracing_subscriber::fmt::try_init();
        let (tx, rx) = channel();

        let mut options = create_client_option();
        options.heartbeat = 2;

        let client = Connection::connect(options).await.unwrap();
        client
            .set_close_callback(Box::new(|| {
                tx.send(()).unwrap();
                async {}
            }))
            .await;

        sleep(Duration::from_secs(2)).await;

        client.close().await.unwrap();

        rx.await.unwrap();
    }

    #[tokio::test]
    async fn test_new_client_drop_connection() {
        let _ = tracing_subscriber::fmt::try_init();
        let (tx, rx) = channel();

        let options = create_client_option();
        let connection_name = options.client_provided_name.clone();
        let client = Connection::connect(options).await.unwrap();
        client
            .set_close_callback(Box::new(|| {
                tx.send(()).unwrap();
                async {}
            }))
            .await;

        let connection = http::wait_till_connection_is_available_on_http(&connection_name)
            .await
            .unwrap();
        http::drop_connection(connection).await;

        sleep(Duration::from_secs(1)).await;

        rx.await.unwrap();
        drop(client);
    }

    #[tokio::test]
    async fn test_new_client_double_close() {
        let _ = tracing_subscriber::fmt::try_init();
        let (tx, rx) = channel();

        let options = create_client_option();
        let client = Connection::connect(options).await.unwrap();
        client
            .set_close_callback(Box::new(|| {
                // This callback is invoked once because:
                // - it is FnOnce
                // - `send` consume `tx`
                // - the channel is closed after this send
                tx.send(()).unwrap();
                async {}
            }))
            .await;

        client.close().await.unwrap();
        client.close().await.unwrap();

        rx.await.unwrap();
    }

    #[tokio::test]
    async fn test_new_client_publish_consume() {
        let _ = tracing_subscriber::fmt::try_init();
        let stream_name: String = Faker.fake();
        let publisher_name: String = Faker.fake();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<MessageResult>(1);

        struct MyMessageHandler {
            tx: tokio::sync::mpsc::Sender<MessageResult>,
        }
        #[async_trait::async_trait]
        impl MessageHandler for MyMessageHandler {
            async fn handle_message(&self, item: MessageResult) -> crate::RabbitMQStreamResult<()> {
                let sender = self.tx.clone();
                sender.send(item).await.unwrap();
                Ok(())
            }
        }

        let options = create_client_option();

        let handler = MyMessageHandler { tx };
        let client = Connection::connect(options).await.unwrap();
        client.set_handle_response_message(Arc::new(handler)).await;

        let mut create_stream_options = HashMap::new();
        create_stream_options.insert(
            "queue-leader-locator".to_string(),
            LeaderLocator::LeastLeaders.as_ref().to_string(),
        );
        let response: GenericResponse = client
            .send_and_receive(|correlation_id| {
                CreateStreamCommand::new(
                    correlation_id,
                    stream_name.to_string(),
                    create_stream_options,
                )
            })
            .await
            .unwrap();
        assert!(response.is_ok());

        let response: GenericResponse = client
            .send_and_receive(|correlation_id| {
                DeclarePublisherCommand::new(
                    correlation_id,
                    1,
                    Some(publisher_name),
                    stream_name.clone(),
                )
            })
            .await
            .unwrap();
        assert!(response.is_ok());

        let response: GenericResponse = client
            .send_and_receive(|correlation_id| {
                SubscribeCommand::new(
                    correlation_id,
                    1,
                    stream_name.to_string(),
                    OffsetSpecification::First,
                    1,
                    HashMap::new(),
                )
            })
            .await
            .unwrap();
        assert!(response.is_ok());

        let mut messages: Vec<PublishedMessage> = Vec::new();
        messages.push(PublishedMessage::new(
            1,
            Message::builder().body("foo").build(),
            None,
        ));
        client
            .send(PublishCommand::new(1, messages, 1))
            .await
            .unwrap();

        sleep(Duration::from_secs(2)).await;

        let deliver_command = loop {
            let response = rx.recv().await.unwrap().unwrap().unwrap();
            let kind = response.kind();
            if let ResponseKind::Deliver(deliver_command) = kind {
                break deliver_command;
            }
        };

        assert_eq!(1, deliver_command.messages.len());
        let data = deliver_command.messages[0].data().unwrap();
        assert_eq!("foo", std::str::from_utf8(data).unwrap());

        client.close().await.unwrap();
    }

    /*
    pub(crate) async fn create_producer_client(
        client_options: ClientOptions,
        stream: &String,
        client_provided_name: String,
    ) -> Result<Arc<NewClient>, ProducerCreateError> {
        let mut opt_with_client_provided_name = client_options.clone();
        opt_with_client_provided_name.client_provided_name = client_provided_name.clone();

        let mut client = NewClient::connect(opt_with_client_provided_name.clone(), None).await?;
        let response: MetadataResponse = client
            .send_and_receive(|correlation_id| {
                MetadataCommand::new(correlation_id, vec![stream.to_string()])
            })
            .await?;
        let medatadata = find_metadata_for_stream(response, stream);

        if let Some(metadata) = medatadata {
            tracing::debug!(
                "Connecting to leader node {:?} of stream {}",
                metadata.leader,
                stream
            );
            let load_balancer_mode = client_options.load_balancer_mode;
            if load_balancer_mode {
                // Producer must connect to leader node
                let options: ClientOptions = client_options.clone();
                loop {
                    let temp_client = NewClient::connect(options.clone(), None).await?;
                    let mapping = temp_client.connection_properties.clone();
                    if let Some(advertised_host) = mapping.get("advertised_host") {
                        if *advertised_host == metadata.leader.host.clone() {
                            client.close().await?;
                            client = temp_client;
                            break;
                        }
                    }
                    temp_client.close().await?;
                }
            } else {
                client.close().await?;
                client = NewClient::connect(
                    ClientOptions {
                        host: metadata.leader.host.clone(),
                        port: metadata.leader.port as u16,
                        ..opt_with_client_provided_name.clone()
                    },
                    None,
                )
                .await?
            };
        } else {
            return Err(ProducerCreateError::StreamDoesNotExist {
                stream: stream.into(),
            });
        }

        Ok(client)
    }
    */

    /*
    fn find_metadata_for_stream(
        response: MetadataResponse,
        stream_name: &String,
    ) -> Option<StreamMetadata> {
        let brokers: HashMap<u16, Broker> = response
            .brokers
            .into_iter()
            .map(|broker| {
                (
                    broker.reference,
                    Broker {
                        host: broker.host,
                        port: broker.port,
                    },
                )
            })
            .collect();

        let metadata = response
            .stream_metadata
            .into_iter()
            .find(|metadata| &metadata.stream_name == stream_name);

        metadata.and_then(|metadata| {
            let leader = brokers.get(&metadata.leader_reference).cloned();
            leader.map(|broker| StreamMetadata {
                stream: metadata.stream_name,
                response_code: metadata.code,
                leader: Broker {
                    host: broker.host,
                    port: broker.port,
                },
                replicas: metadata
                    .replicas_references
                    .into_iter()
                    .filter_map(|replica| brokers.get(&replica).cloned())
                    .collect(),
            })
        })
    }
    */

    fn create_client_option() -> ClientOptions {
        let connection_name: String = Faker.fake();

        let mut options = ClientOptions::default();
        options.client_provided_name = connection_name.clone();

        options
    }

    mod http {
        use std::{collections::HashMap, time::Duration};

        use serde::{Deserialize, Serialize};
        use tokio::time::sleep;

        static RABBITMQ_HTTP_PORT: u16 = 15672;

        #[derive(Debug, Serialize, Deserialize)]
        pub struct Connection {
            pub name: String,
            pub peer_port: u16,
            pub client_properties: HashMap<String, String>,
        }

        pub async fn list_connections() -> Vec<Connection> {
            let client = reqwest::Client::new();
            let url = format!("http://localhost:{}/api/connections/", RABBITMQ_HTTP_PORT);
            let response = client
                .get(url)
                .basic_auth("guest", Some("guest"))
                .send()
                .await
                .unwrap();
            let response = response.error_for_status().unwrap();
            response.json().await.unwrap()
        }

        pub async fn wait_till_connection_is_available_on_http(
            connection_name: &String,
        ) -> Option<Connection> {
            let mut retries = 10;
            while retries > 0 {
                let connections = list_connections().await;

                let connection = connections
                    .into_iter()
                    .find(|c| c.client_properties.get("connection_name") == Some(connection_name));
                if let Some(connection) = connection {
                    return Some(connection);
                }
                sleep(Duration::from_millis(400)).await;
                retries -= 1;
            }

            None
        }

        pub async fn drop_connection(connection: Connection) {
            let client = reqwest::Client::new();
            let url = format!(
                "http://localhost:{}/api/connections/{}",
                RABBITMQ_HTTP_PORT, connection.name
            );
            let response = client
                .delete(url)
                .basic_auth("guest", Some("guest"))
                .send()
                .await
                .unwrap();
            response.error_for_status().unwrap();
        }
    }
}
