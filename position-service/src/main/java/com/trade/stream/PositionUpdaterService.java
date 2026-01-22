package com.trade.stream;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import redis.clients.jedis.Jedis;

import com.trade.stream.common.Common;
import com.google.protobuf.util.JsonFormat;

import java.time.Duration;
import java.util.*;

public class PositionUpdaterService {

    private static final String KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String INQUIRY_TOPIC = "inquiry-topic";
    private static final Jedis redis = new Jedis("localhost", 6379);

    private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

    public static void main(String[] args) {

        // Kafka consumer config
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "position-updater-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Kafka producer config
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

        consumer.subscribe(Collections.singletonList(INQUIRY_TOPIC));

        System.out.println("PositionUpdaterService started, listening to " + INQUIRY_TOPIC);

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, byte[]> record : records) {
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
                    e.printStackTrace();
                }
            }
        }
    }

    private static void handleNewInquiry(Common.Inquiry inquiry, KafkaProducer<String, byte[]> producer) throws Exception {
        // Update position in memory/Redis (optional)
        String key = "position:" + inquiry.getInstrumentId() + ":" + inquiry.getBookId();

        // Increment position in Redis
        long position = redis.hincrBy(key, "position", 0);

        // Increment version of inquiry
        Common.Inquiry updatedInquiry = inquiry.toBuilder()
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

    private static void handleDoneInquiry(Common.Inquiry inquiry) {
        String key = "position:" + inquiry.getInstrumentId() + ":" + inquiry.getBookId();
        long qty = inquiry.getQuantity();
        long price = inquiry.getPrice();
        // Update Redis
        redis.hincrBy(key, "position", (qty*price));

        try {
            String json = JSON_PRINTER.print(inquiry);
            System.out.println("Processed DONE inquiry, updated Redis: " + json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
