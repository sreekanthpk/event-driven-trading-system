package com.trade.stream;


import com.trade.stream.common.Common;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class InquiryAutoTraderService {

    private static final String KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String INQUIRY_TOPIC = "inquiry-topic";
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {

        // ----------------- Kafka consumer config -----------------
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "inquiry-auto-trader-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // ----------------- Kafka producer config -----------------
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

        consumer.subscribe(Collections.singletonList(INQUIRY_TOPIC));

        System.out.println("InquiryAutoTraderService started, listening to " + INQUIRY_TOPIC);

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    Common.Inquiry inquiry = Common.Inquiry.parseFrom(record.value());

                    if(inquiry.getStatus() != Common.Enums.Status.POSITION_ENRICHED) continue;

                    // Decide status: 1 in 10 DONE, rest NOT_DONE
                    Common.Enums.Status newStatus = (RANDOM.nextInt(10) == 0)
                            ? Common.Enums.Status.DONE
                            : Common.Enums.Status.NOT_DONE;

                    Common.Inquiry updatedInquiry = inquiry.toBuilder()
                            .setVersion(inquiry.getVersion() + 1)
                            .setStatus(newStatus)
                            .build();


                    // Publish updated inquiry back to Kafka
                    producer.send(new ProducerRecord<>(INQUIRY_TOPIC, updatedInquiry.toByteArray()));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
