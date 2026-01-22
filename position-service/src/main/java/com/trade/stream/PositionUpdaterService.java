package com.trade.stream;

import static com.trade.stream.CommonConstants.*;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.trade.stream.common.Common;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import redis.clients.jedis.Jedis;

public class PositionUpdaterService {

  private static final Jedis redis = new Jedis("localhost", 6379);

  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

  public static void main(String[] args) {

    try (Consumer<Long, byte[]> consumer =
        KafkaUtil.createConsumer(KAFKA_BOOTSTRAP, "position-service")) {
      Producer<Long, byte[]> producer = KafkaUtil.createProducer(KAFKA_BOOTSTRAP);

      consumer.subscribe(Collections.singletonList(INQUIRY_TOPIC));

      System.out.println("PositionUpdaterService started, listening to " + INQUIRY_TOPIC);

      while (true) {
        ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<Long, byte[]> record : records) {
          try {
            Common.Inquiry inquiry = Common.Inquiry.parseFrom(record.value());

            switch (inquiry.getStatus()) {
              case NEW:
                handleNewInquiry(inquiry, producer);
                break;

              case DONE:
                handleDoneInquiry(inquiry);
                break;
              default:
                // ignore
            }

          } catch (Exception e) {
            System.out.println(e.getMessage());
          }
        }
      }
    }
  }

  private static void handleNewInquiry(Common.Inquiry inquiry, Producer<Long, byte[]> producer)
      throws InvalidProtocolBufferException {
    // Update position in memory/Redis (optional)
    String key = "position:" + inquiry.getInstrumentId() + ":" + inquiry.getBookId();

    // Increment position in Redis
    long position = redis.hincrBy(key, "position", 0);

    // Increment version of inquiry
    Common.Inquiry updatedInquiry =
        inquiry.toBuilder()
            .setVersion(inquiry.getVersion() + 1)
            .setPosition(position)
            .setStatus(Common.Enums.Status.POSITION_ENRICHED)
            .build();

    // Publish updated inquiry back to Kafka
    producer.send(new ProducerRecord<>(INQUIRY_TOPIC, updatedInquiry.toByteArray()));

    // Log for observability
    String json = JSON_PRINTER.print(updatedInquiry);
    System.out.println("Processed NEW inquiry, updated and sent: " + json);
  }

  private static void handleDoneInquiry(Common.Inquiry inquiry)
      throws InvalidProtocolBufferException {
    String key = "position:" + inquiry.getInstrumentId() + ":" + inquiry.getBookId();
    long qty = inquiry.getQuantity();
    long price = inquiry.getPrice();
    // Update Redis
    redis.hincrBy(key, "position", (qty * price));
    String json = JSON_PRINTER.print(inquiry);
    System.out.println("Processed DONE inquiry, updated Redis: " + json);
  }
}
