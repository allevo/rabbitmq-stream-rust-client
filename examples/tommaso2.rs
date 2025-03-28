use rabbitmq_stream_client::environment2::Environment2;
use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::superstream2::{HashRoutingMurmurStrategy, RoutingStrategy};
use rabbitmq_stream_client::types::{
    ByteCapacity, Message, ResponseCode,
};
use std::convert::TryInto;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

fn hash_strategy_value_extractor(message: &Message) -> String {
    message
        .application_properties()
        .unwrap()
        .get("id")
        .unwrap()
        .clone()
        .try_into()
        .unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let environment = Environment2::builder()
        .load_balancer_mode(true)
        .host("34.147.193.13")
        .username("test")
        .password("test")
        .build().await?;
    let message_count = 3;
    let super_stream = "tommaso-pippo";
    let confirmed_messages = Arc::new(AtomicU32::new(0));
    let notify_on_send = Arc::new(Notify::new());

    /*
    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create_super_stream(super_stream, 1, None)
        .await;
    if let Err(e) = create_response {
        if let StreamCreateError::Create { stream, status } = e {
            match status {
                // we can ignore this error because the stream already exists
                ResponseCode::StreamAlreadyExists => {}
                err => {
                    println!("Error creating stream: {:?} {:?}", stream, err);
                }
            }
        }
    }
    */
    println!(
        "Super stream example. Sending {} messages to the super stream: {}",
        message_count, super_stream
    );
    let mut super_stream_producer = environment
        .super_stream_producer(RoutingStrategy::HashRoutingStrategy(
            HashRoutingMurmurStrategy {
                routing_extractor: &hash_strategy_value_extractor,
            },
        ))
        .client_provided_name("my super stream producer for hello rust")
        .build(super_stream)
        .await
        .unwrap();

    for i in 0..message_count {
        let counter = confirmed_messages.clone();
        let notifier = notify_on_send.clone();
        let msg = Message::builder()
            .body(format!("super stream message_{}", i))
            .application_properties()
            .insert("id", i.to_string())
            .message_builder()
            .build();
        super_stream_producer
            .send(msg, move |_| {
                let inner_counter = counter.clone();
                let inner_notifier = notifier.clone();
                async move {
                    if inner_counter.fetch_add(1, Ordering::Relaxed) == message_count - 1 {
                        inner_notifier.notify_one();
                    }
                }
            })
            .await
            .unwrap();
    }

    notify_on_send.notified().await;
    println!(
        "Successfully sent {} messages to the super stream {}",
        message_count, super_stream
    );
    let _ = super_stream_producer.close().await;
    Ok(())
}
