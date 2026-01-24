package com.trade.stream;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaUtil {

  public static Producer<Long, byte[]> createProducer(String bootstrapServers) {
    Properties props = CommonConstants.getProducerProps();

    // Override bootstrap servers if provided
    if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
      props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    return new KafkaProducer<>(props);
  }

  public static Consumer<Long, byte[]> createConsumer(String bootstrapServers, String groupId) {
    if (groupId == null || groupId.isEmpty()) {
      throw new IllegalArgumentException("groupId cannot be null or empty");
    }

    Properties props = CommonConstants.getConsumerProps();
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    // Override bootstrap servers if provided
    if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
      props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    return new KafkaConsumer<>(props);
  }
}
