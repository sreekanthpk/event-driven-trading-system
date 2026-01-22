package com.trade.stream;


import com.trade.stream.common.Common;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Random;

import static com.trade.stream.CommonConstants.*;

public class InquiryAutoTraderService {

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {

        try (Consumer<Long, byte[]> consumer = KafkaUtil.createConsumer(KAFKA_BOOTSTRAP)) {
            try (Producer<Long, byte[]> producer = KafkaUtil.createProducer(KAFKA_BOOTSTRAP)) {

                consumer.subscribe(Collections.singletonList(INQUIRY_TOPIC));

                System.out.println("InquiryAutoTraderService started, listening to " + INQUIRY_TOPIC);

                while (true) {
                    ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(500));

                    for (ConsumerRecord<Long, byte[]> record : records) {
                        try {
                            Common.Inquiry inquiry = Common.Inquiry.parseFrom(record.value());

                            if (inquiry.getStatus() != Common.Enums.Status.POSITION_ENRICHED) continue;

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
                            System.out.println("InquiryAutoTraderService error: " + e.getMessage());
                        }
                    }
                }
            }
        }
    }
}
