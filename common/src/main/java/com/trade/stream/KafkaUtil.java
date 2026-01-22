package com.trade.stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;



import static com.trade.stream.CommonConstants.consumerProps;
import static com.trade.stream.CommonConstants.producerProps;

public class KafkaUtil {
    public static Producer<Long, byte[]> createProducer(String bootstrapServers) {
        return new KafkaProducer<>(producerProps);
    }

    public static Consumer<Long, byte[]> createConsumer(String bootstrapServers) {
        return new KafkaConsumer<>(consumerProps);
    }
}
