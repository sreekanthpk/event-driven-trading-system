package com.trade.stream;

import com.google.protobuf.util.JsonFormat;
import com.trade.stream.common.Common;
import io.vertx.core.AbstractVerticle;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerVerticle extends AbstractVerticle {

    private static final JsonFormat.Printer JSON_PRINTER =
            JsonFormat.printer().omittingInsignificantWhitespace();

    @Override
    public void start() {

        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        config.put("group.id", "ws-ui-group");
        config.put("auto.offset.reset", "latest");
        config.put("enable.auto.commit", "true");

        KafkaConsumer<String, byte[]> consumer =
                KafkaConsumer.create(vertx, config);

        consumer.subscribe("inquiry-topic");

        consumer.handler(this::handleRecord);
    }

    private void handleRecord(KafkaConsumerRecord<String, byte[]> record) {
        byte[] bytes = record.value();

        try {
            // Deserialize ONLY for logging
            Common.Inquiry inquiry = Common.Inquiry.parseFrom(bytes);

            // Convert to JSON for logs
            String json = JSON_PRINTER.print(inquiry);

            System.out.println(String.format("Kafka Inquiry received: %s", json));

        } catch (Exception e) {
            System.out.println("Failed to deserialize Inquiry protobuf" + e);
        }

        // Publish original bytes unchanged
        vertx.eventBus().publish("ui.updates", bytes);
    }
}
