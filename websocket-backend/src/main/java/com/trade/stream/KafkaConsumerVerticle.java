package com.trade.stream;

import static com.trade.stream.CommonConstants.INQUIRY_TOPIC;
import static com.trade.stream.CommonConstants.KAFKA_BOOTSTRAP;

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

  private KafkaConsumer<Long, byte[]> consumer;
  private int connectedClients = 0;
  private boolean subscribed = false;

  @Override
  public void start() {

    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", KAFKA_BOOTSTRAP);
    config.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    config.put("group.id", "ws-ui-group");
    config.put("auto.offset.reset", "earliest");
    config.put("enable.auto.commit", "true");

    consumer = KafkaConsumer.create(vertx, config);
    consumer.handler(this::handleRecord);
    vertx.eventBus().consumer("ui.client.connected", msg -> onClientConnected());
    vertx.eventBus().consumer("ui.client.disconnected", msg -> onClientDisconnected());
  }

  private void onClientConnected() {
    connectedClients++;

    if (!subscribed) {
      consumer.subscribe(INQUIRY_TOPIC);
      subscribed = true;
      System.out.println("Kafka consumer subscribed (first client connected)");
    }
  }

  private void onClientDisconnected() {
    connectedClients--;

    if (connectedClients <= 0 && subscribed) {
      consumer.unsubscribe();
      subscribed = false;
      connectedClients = 0;
      System.out.println("Kafka consumer unsubscribed (no clients)");
    }
  }

  private void handleRecord(KafkaConsumerRecord<Long, byte[]> record) {
    byte[] bytes = record.value();

    try {
      Common.Inquiry inquiry = Common.Inquiry.parseFrom(bytes);

      String json = JSON_PRINTER.print(inquiry);

      System.out.println(String.format("Kafka Inquiry received: %s", json));

    } catch (Exception e) {
      System.out.println("Failed to deserialize Inquiry protobuf" + e);
    }

    vertx.eventBus().publish("ui.updates", bytes);
  }
}
