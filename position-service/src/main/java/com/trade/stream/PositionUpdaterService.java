package com.trade.stream;

import static com.trade.stream.CommonConstants.*;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.trade.stream.common.Common;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class PositionUpdaterService {

  private static final Logger log = LoggerFactory.getLogger(PositionUpdaterService.class);
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
  private static volatile boolean running = true;

  public static void main(String[] args) {

    // Shutdown hook for graceful stop
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutting down PositionUpdaterService...");
      running = false;
    }));

    try (Consumer<Long, byte[]> consumer = KafkaUtil.createConsumer(KAFKA_BOOTSTRAP, "position-service");
         Producer<Long, byte[]> producer = KafkaUtil.createProducer(KAFKA_BOOTSTRAP);
         Jedis redis = new Jedis("localhost", 6379)) {

      consumer.subscribe(Collections.singletonList(INQUIRY_TOPIC));

      log.info("PositionUpdaterService started, listening to {}", INQUIRY_TOPIC);

      while (running) {
        ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<Long, byte[]> record : records) {
          try {
            Common.Inquiry inquiry = Common.Inquiry.parseFrom(record.value());

            switch (inquiry.getStatus()) {
              case NEW:
                handleNewInquiry(inquiry, producer, redis);
                break;

              case DONE:
                handleDoneInquiry(inquiry, redis);
                break;
              default:
                // ignore
            }

          } catch (Exception e) {
            log.error("Error processing record: {}", e.getMessage(), e);
          }
        }
      }

      log.info("PositionUpdaterService stopped");

    } catch (Exception e) {
      log.error("Fatal error in PositionUpdaterService", e);
    }
  }

  private static void handleNewInquiry(Common.Inquiry inquiry, Producer<Long, byte[]> producer, Jedis redis)
          throws InvalidProtocolBufferException {
    String key = "position:" + inquiry.getInstrumentId() + ":" + inquiry.getBookId();

    // Get current position from Redis
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
    log.info("Processed NEW inquiry: {}", json);
  }

  private static void handleDoneInquiry(Common.Inquiry inquiry, Jedis redis)
          throws InvalidProtocolBufferException {
    String key = "position:" + inquiry.getInstrumentId() + ":" + inquiry.getBookId();
    long qty = inquiry.getQuantity();
    long price = inquiry.getPrice();
    long position = qty * price;

    if (inquiry.getSide() == Common.Enums.Side.SELL) {
      position = -position;
    }

    redis.hincrBy(key, "position", position);

    String json = JSON_PRINTER.print(inquiry);
    log.info("Processed DONE inquiry, updated Redis: {}", json);
  }
}