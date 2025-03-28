use std::time::Duration;

#[path = "./common.rs"]
mod common;

use common::*;

use fake::{Fake, Faker};
use futures::StreamExt;
use rabbitmq_stream_client::types::{HashRoutingMurmurStrategy, Message, OffsetSpecification, RoutingStrategy, SuperStreamConsumer};

use rabbitmq_stream_protocol::ResponseCode;
use tracing::warn;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::{sync::Notify, time::sleep};
use tokio::task;
use {std::sync::Arc, std::sync::Mutex};

pub fn routing_key_strategy_value_extractor(_: &Message) -> String {
    "0".to_string()
}

fn hash_strategy_value_extractor(message: &Message) -> String {
    let s = String::from_utf8(Vec::from(message.data().unwrap())).expect("Found invalid UTF-8");
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn old_super_stream_single_active_consumer_test_1() {
    let _ = tracing_subscriber::fmt::try_init();

    // warn!("create env");
    let env = TestEnvironment::create_super_stream().await;

    let message_count = 3;
    // warn!("create super_stream_producer 1");
    let mut super_stream_producer = env
        .env
        .super_stream_producer(RoutingStrategy::HashRoutingStrategy(
            HashRoutingMurmurStrategy {
                routing_extractor: &hash_strategy_value_extractor,
            },
        ))
        .client_provided_name("test super stream consumer ")
        .build(&env.super_stream)
        .await
        .unwrap();

    let notify_received_messages = Arc::new(Notify::new());

    sleep(Duration::from_millis(1_000)).await;
    println!("----");
    sleep(Duration::from_millis(1_000)).await;

    for n in 0..message_count {
        let msg = Message::builder()
            .body(format!("message{}", n)).build();
        // warn!("send message {}", n);
        super_stream_producer
            .send(msg, |_confirmation_status| async move {})
            .await
            .unwrap();
    }

    // warn!("create super_stream_consumer ");
    let mut super_stream_consumer: SuperStreamConsumer = env
        .env
        .super_stream_consumer()
        .enable_single_active_consumer(true)
        .name("super-stream-with-sac-enabled")
        .offset(OffsetSpecification::First)
        .build(&env.super_stream)
        .await
        .unwrap();

    sleep(Duration::from_millis(1_000)).await;
    println!("----");
    sleep(Duration::from_millis(1_000)).await;

    let received_messages = Arc::new(AtomicU32::new(1));
    let handle_consumer_1 = super_stream_consumer.handle();

    let received_message_outer = received_messages.clone();
    let notify_received_messages_outer = notify_received_messages.clone();
    task::spawn(async move {
        let received_messages_int = received_message_outer.clone();
        let notify_received_messages_inner = notify_received_messages_outer.clone();
        while let Some(_) = super_stream_consumer.next().await {
            // warn!("AAAA");
            let rec_msg = received_messages_int.fetch_add(1, Ordering::Relaxed);
            if message_count == rec_msg {
                notify_received_messages_inner.notify_one();
                break;
            }
        }
        panic!("OUCH");
    });

    // warn!("wait for notify");
    notify_received_messages.notified().await;

    assert!(received_messages.load(Ordering::Relaxed) == message_count + 1);

    super_stream_producer.close().await.unwrap();
    let _ = handle_consumer_1.close().await;
}
