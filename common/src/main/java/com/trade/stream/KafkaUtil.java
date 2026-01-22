package com.trade.stream;

import static com.trade.stream.CommonConstants.consumerProps;
import static com.trade.stream.CommonConstants.producerProps;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class KafkaUtil {
  public static Producer<Long, byte[]> createProducer(String bootstrapServers) {
    return new KafkaProducer<>(producerProps);
  }

  public static Consumer<Long, byte[]> createConsumer(String bootstrapServers, String groupId) {
    Properties newConsumerProp = new Properties();
    newConsumerProp.putAll(consumerProps);
    newConsumerProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    return new KafkaConsumer<>(newConsumerProp);
  }
}
