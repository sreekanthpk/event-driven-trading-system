package com.trade.stream;

import org.apache.kafka.clients.producer.Producer;

import static com.trade.stream.CommonConstants.INQUIRY_TOPIC;
import static com.trade.stream.CommonConstants.KAFKA_BOOTSTRAP;

public class InquiryProducerApp {

    public static void main(String[] args) throws Exception {

        Producer<Long, byte[]> producer = KafkaUtil.createProducer(KAFKA_BOOTSTRAP);


        RandomInquiryGenerator generator =
                new RandomInquiryGenerator(producer, INQUIRY_TOPIC);

        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        while (true) {
            generator.sendRandomInquiry();
            Thread.sleep(1000);
        }
    }
}