use futures::StreamExt;
use rabbitmq_stream_client::environment2::Environment2;
use rabbitmq_stream_client::error::StreamCreateError;
use rabbitmq_stream_client::superstream_consumer2::SuperStreamConsumer2;
use rabbitmq_stream_client::types::{
    ByteCapacity, OffsetSpecification, ResponseCode, SuperStreamConsumer,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();

    let environment = Environment2::builder()
        .load_balancer_mode(true)
        .host("34.147.193.13")
        .username("test")
        .password("test")
        .build().await?;
    let super_stream = "tommaso-pippo";

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
    println!(
        "Super stream consumer example, consuming messages from the super stream {}",
        super_stream
    );
    let mut super_stream_consumer: SuperStreamConsumer2 = environment
        .super_stream_consumer()
        .offset(OffsetSpecification::First)
        .enable_single_active_consumer(true)
        .client_provided_name("bar")
        .name("foo")
        .consumer_update(|p, a| async move {
            OffsetSpecification::First
        })
        .build(super_stream)
        .await
        .unwrap();

    while let Some(Ok(delivery)) = super_stream_consumer.next().await {
        println!(
            "Got message: {:#?} from stream: {} with offset: {}",
            delivery
                .message()
                .data()
                .map(|data| String::from_utf8(data.to_vec()).unwrap())
                .unwrap(),
            delivery.stream(),
            delivery.offset()
        );
    }

    println!("Stopping super stream consumer...");
    let _ = super_stream_consumer.handle().close().await;
    println!("Super stream consumer stopped");
    Ok(())
}
