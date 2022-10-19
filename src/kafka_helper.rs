use rdkafka::{ClientContext, consumer::{ConsumerContext, Rebalance}, error::KafkaResult, TopicPartitionList};
use rdkafka::consumer::stream_consumer::StreamConsumer;

pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        log::info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        log::info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        log::info!("Committing offsets: {:?}", result);
    }
}

pub type LoggingConsumer = StreamConsumer<CustomContext>;
