package com.trade.stream;

import static com.trade.stream.CommonConstants.INQUIRY_TOPIC;
import static com.trade.stream.CommonConstants.KAFKA_BOOTSTRAP;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InquiryProducerApp {

  private static final Logger log = LoggerFactory.getLogger(InquiryProducerApp.class);
  private static volatile boolean running = true;

  public static void main(String[] args) {
    Producer<Long, byte[]> producer = KafkaUtil.createProducer(KAFKA_BOOTSTRAP);
    RandomInquiryGenerator generator = new RandomInquiryGenerator(producer, INQUIRY_TOPIC);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutting down InquiryProducerApp...");
      running = false;
      producer.close();
      log.info("Producer closed");
    }));

    log.info("InquiryProducerApp started, sending inquiries every 1 second");

    try {
      while (running) {
        generator.sendRandomInquiry();
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while sleeping", e);
    }

    log.info("InquiryProducerApp stopped");
  }
}