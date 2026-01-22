package com.trade.stream;

import org.apache.kafka.clients.producer.Producer;

public class InquiryProducerApp {

    public static void main(String[] args) throws Exception {

        String bootstrapServers = "localhost:9092";
        String topic = "inquiry-topic";

        Producer<Long, byte[]> producer =
                KafkaProducerConfig.createProducer(bootstrapServers);

        RandomInquiryGenerator generator =
                new RandomInquiryGenerator(producer, topic);

        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        while (true) {
            generator.sendRandomInquiry();
            Thread.sleep(1000); // 1 inquiry per second
            System.out.println("Sent random inquiry");
        }
    }
}