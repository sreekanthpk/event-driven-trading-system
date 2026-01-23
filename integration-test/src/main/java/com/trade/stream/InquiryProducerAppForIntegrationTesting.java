package com.trade.stream;

import static com.trade.stream.CommonConstants.INQUIRY_TOPIC;
import static com.trade.stream.CommonConstants.KAFKA_BOOTSTRAP;

import org.apache.kafka.clients.producer.Producer;

public class InquiryProducerAppForIntegrationTesting {

  public static void main(String[] args) throws Exception {

    Producer<Long, byte[]> producer = KafkaUtil.createProducer(KAFKA_BOOTSTRAP);

    InquiryGeneratorForIntegrationTesting generator =
        new InquiryGeneratorForIntegrationTesting(producer, INQUIRY_TOPIC);

    Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

    for (int i = 0; i < 20; i++) {
      generator.sendInquiry(i);
      Thread.sleep(1000);
    }
  }
}
