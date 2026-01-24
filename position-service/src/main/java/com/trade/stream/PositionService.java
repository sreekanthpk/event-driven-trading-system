package com.trade.stream;

import static com.trade.stream.CommonConstants.*;

import com.trade.stream.common.Common;
import java.util.Collections;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class PositionService {
  private static final Logger log = LoggerFactory.getLogger(PositionService.class);

  private static volatile boolean running = true;

  public static void main(String[] args) {

    String POSITION_GROUP_ID = "position-app";

    try (Jedis redis = new Jedis("localhost", 6379);
        Consumer<Long, byte[]> consumer =
            KafkaUtil.createConsumer(KAFKA_BOOTSTRAP, POSITION_GROUP_ID)) {

      consumer.subscribe(Collections.singletonList(INQUIRY_TOPIC));

      Producer<Long, byte[]> producer = KafkaUtil.createProducer(KAFKA_BOOTSTRAP);

      PositionProcessor processor = new PositionProcessor(redis, producer, INQUIRY_TOPIC);

      log.info("PositionProcessorApp started");

      while (running) {
        ConsumerRecords<Long, byte[]> records = consumer.poll(java.time.Duration.ofMillis(100));

        for (ConsumerRecord<Long, byte[]> record : records) {
          try {
            Common.Inquiry inquiry = Common.Inquiry.parseFrom(record.value());
            processor.processInquiry(inquiry);
          } catch (Exception e) {
            log.error("Error processing inquiry", e);
          }
        }
      }
    } finally {
      running = false;
    }

    log.info("PositionService stopped");
  }
}
