package com.trade.stream;

import static com.trade.stream.CommonConstants.*;

import com.trade.stream.common.Common;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InquiryAutoTraderService {

  private static final Logger log = LoggerFactory.getLogger(InquiryAutoTraderService.class);
  private static volatile boolean running = true;

  public static void main(String[] args) {

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Shutting down InquiryAutoTraderService...");
                  running = false;
                }));

    try (Consumer<Long, byte[]> consumer =
            KafkaUtil.createConsumer(KAFKA_BOOTSTRAP, "auto-trader");
        Producer<Long, byte[]> producer = KafkaUtil.createProducer(KAFKA_BOOTSTRAP)) {

      consumer.subscribe(Collections.singletonList(INQUIRY_TOPIC));

      log.info("InquiryAutoTraderService started, listening to {}", INQUIRY_TOPIC);

      while (running) {
        ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<Long, byte[]> record : records) {
          try {
            Common.Inquiry inquiry = Common.Inquiry.parseFrom(record.value());

            if (inquiry.getStatus() != Common.Enums.Status.POSITION_ENRICHED) continue;

            // Decide status: 1 in 10 DONE, rest NOT_DONE
            Common.Enums.Status newStatus =
                (ThreadLocalRandom.current().nextInt(10) == 0)
                    ? Common.Enums.Status.DONE
                    : Common.Enums.Status.NOT_DONE;

            Common.Inquiry updatedInquiry =
                inquiry.toBuilder()
                    .setVersion(inquiry.getVersion() + 1)
                    .setStatus(newStatus)
                    .build();

            // Publish updated inquiry back to Kafka
            producer.send(new ProducerRecord<>(INQUIRY_TOPIC, updatedInquiry.toByteArray()));

            log.debug("Processed inquiry {} with status {}", inquiry.getInquiryId(), newStatus);

          } catch (Exception e) {
            log.error("Error processing inquiry: {}", e.getMessage(), e);
          }
        }
      }

      log.info("InquiryAutoTraderService stopped");

    } catch (Exception e) {
      log.error("Fatal error in InquiryAutoTraderService", e);
    }
  }
}
