package com.trade.stream;

import static com.trade.stream.CommonConstants.INQUIRY_TOPIC;
import static com.trade.stream.CommonConstants.KAFKA_BOOTSTRAP;

import com.google.protobuf.util.JsonFormat;
import com.trade.stream.common.Common;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumerVerticle extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerVerticle.class);
  private static final JsonFormat.Printer JSON_PRINTER =
          JsonFormat.printer().omittingInsignificantWhitespace();

  private static final int MAX_PENDING_MESSAGES = 1000;
  private static final long BACKPRESSURE_CHECK_INTERVAL_MS = 1000;

  private KafkaConsumer<Long, byte[]> consumer;
  private final AtomicInteger connectedClients = new AtomicInteger(0);
  private final AtomicBoolean subscribed = new AtomicBoolean(false);
  private final AtomicInteger pendingMessages = new AtomicInteger(0);

  @Override
  public void start(Promise<Void> startPromise) {
    try {
      Map<String, String> kafkaConfig = buildKafkaConfig();

      consumer = KafkaConsumer.create(vertx, kafkaConfig);
      consumer.handler(this::handleRecord);
      consumer.exceptionHandler(this::handleKafkaError);
      consumer.partitionsAssignedHandler(partitions ->
              log.info("Partitions assigned: {}", partitions));
      consumer.partitionsRevokedHandler(partitions ->
              log.info("Partitions revoked: {}", partitions));

      vertx.eventBus().consumer("ui.client.connected", msg -> onClientConnected());
      vertx.eventBus().consumer("ui.client.disconnected", msg -> onClientDisconnected());

      log.info("KafkaConsumerVerticle started successfully");
      startPromise.complete();

    } catch (Exception e) {
      log.error("Failed to start KafkaConsumerVerticle", e);
      startPromise.fail(e);
    }
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    log.info("Stopping KafkaConsumerVerticle...");

    if (consumer != null) {
      consumer.close(result -> {
        if (result.succeeded()) {
          log.info("Kafka consumer closed successfully");
          stopPromise.complete();
        } else {
          log.error("Error closing Kafka consumer", result.cause());
          stopPromise.fail(result.cause());
        }
      });
    } else {
      stopPromise.complete();
    }
  }

  private Map<String, String> buildKafkaConfig() {
    JsonObject config = config();

    Map<String, String> kafkaConfig = new HashMap<>();
    kafkaConfig.put("bootstrap.servers",
            config.getString("kafka.bootstrap.servers", KAFKA_BOOTSTRAP));
    kafkaConfig.put("key.deserializer",
            "org.apache.kafka.common.serialization.LongDeserializer");
    kafkaConfig.put("value.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    kafkaConfig.put("group.id",
            config.getString("kafka.consumer.group.id", "ws-ui-group"));
    kafkaConfig.put("auto.offset.reset",
            config.getString("kafka.consumer.auto.offset.reset", "earliest"));
    kafkaConfig.put("enable.auto.commit", "false"); // Manual commit for reliability
    kafkaConfig.put("max.poll.records", "100");
    kafkaConfig.put("session.timeout.ms", "30000");
    kafkaConfig.put("heartbeat.interval.ms", "10000");

    return kafkaConfig;
  }

  private void onClientConnected() {
    int clients = connectedClients.incrementAndGet();
    log.debug("Client connected. Total clients: {}", clients);

    if (clients == 1 && subscribed.compareAndSet(false, true)) {
      consumer.subscribe(INQUIRY_TOPIC, result -> {
        if (result.succeeded()) {
          log.info("Kafka consumer subscribed to topic: {}", INQUIRY_TOPIC);
        } else {
          log.error("Failed to subscribe to Kafka topic: {}", INQUIRY_TOPIC,
                  result.cause());
          subscribed.set(false);
        }
      });
    }
  }

  private void onClientDisconnected() {
    int clients = connectedClients.decrementAndGet();
    log.debug("Client disconnected. Remaining clients: {}", clients);

    if (clients <= 0 && subscribed.compareAndSet(true, false)) {
      connectedClients.set(0); // Prevent negative values

      consumer.unsubscribe(result -> {
        if (result.succeeded()) {
          log.info("Kafka consumer unsubscribed from topic: {}", INQUIRY_TOPIC);
        } else {
          log.error("Failed to unsubscribe from Kafka topic: {}", INQUIRY_TOPIC,
                  result.cause());
          subscribed.set(true);
        }
      });
    }
  }

  private void handleRecord(KafkaConsumerRecord<Long, byte[]> record) {
    // Check backpressure
    int pending = pendingMessages.get();
    if (pending >= MAX_PENDING_MESSAGES) {
      log.warn("Backpressure limit reached. Pending: {}. Pausing consumer.", pending);
      consumer.pause();
      scheduleConsumerResume();
      return;
    }

    pendingMessages.incrementAndGet();

    try {
      Common.Inquiry inquiry = Common.Inquiry.parseFrom(record.value());

      // Set up MDC for logging context
      String correlationId = String.valueOf(inquiry.getInquiryId());
      MDC.put("correlationId", correlationId);
      MDC.put("partition", String.valueOf(record.partition()));
      MDC.put("offset", String.valueOf(record.offset()));

      try {
        String json = JSON_PRINTER.print(inquiry);
        log.info("Kafka Inquiry received: inquiryId={}, symbol={}, side={}, quantity={}",
                inquiry.getInquiryId(),
                inquiry.getInstrumentId(),
                inquiry.getSide(),
                inquiry.getQuantity());

        if (log.isDebugEnabled()) {
          log.debug("Full inquiry JSON: {}", json);
        }

        // Publish to event bus with correlation ID
        DeliveryOptions options = new DeliveryOptions()
                .addHeader("correlationId", correlationId);

        vertx.eventBus().publish("ui.updates", record.value(), options);

        // Manual commit after successful processing
        consumer.commit(commitResult -> {
          if (commitResult.failed()) {
            log.error("Failed to commit offset for partition={} offset={}",
                    record.partition(), record.offset(), commitResult.cause());
          }
        });

      } finally {
        MDC.clear();
        pendingMessages.decrementAndGet();
      }

    } catch (Exception e) {
      log.error("Failed to deserialize Inquiry protobuf from partition={} offset={}",
              record.partition(), record.offset(), e);

      publishToDeadLetterQueue(record, e);
      pendingMessages.decrementAndGet();

      // Still commit to avoid reprocessing poison pill
      consumer.commit();
    }
  }

  private void handleKafkaError(Throwable error) {
    log.error("Kafka consumer error: {}", error.getMessage(), error);

    // Attempt to resubscribe after delay
    if (subscribed.get()) {
      vertx.setTimer(5000, id -> {
        log.info("Attempting to resubscribe to Kafka topic: {}", INQUIRY_TOPIC);
        consumer.subscribe(INQUIRY_TOPIC, result -> {
          if (result.succeeded()) {
            log.info("Successfully resubscribed to topic: {}", INQUIRY_TOPIC);
          } else {
            log.error("Failed to resubscribe to topic: {}", INQUIRY_TOPIC,
                    result.cause());
          }
        });
      });
    }
  }

  private void scheduleConsumerResume() {
    vertx.setTimer(BACKPRESSURE_CHECK_INTERVAL_MS, id -> {
      int pending = pendingMessages.get();
      if (pending < MAX_PENDING_MESSAGES / 2) {
        consumer.resume();
        log.info("Consumer resumed. Pending messages: {}", pending);
      } else {
        // Check again later
        scheduleConsumerResume();
      }
    });
  }

  private void publishToDeadLetterQueue(KafkaConsumerRecord<Long, byte[]> record, Exception error) {
    JsonObject dlqMessage = new JsonObject()
            .put("topic", record.topic())
            .put("partition", record.partition())
            .put("offset", record.offset())
            .put("key", record.key())
            .put("timestamp", record.timestamp())
            .put("error", error.getMessage())
            .put("errorType", error.getClass().getSimpleName());

    vertx.eventBus().publish("dlq.inquiry", dlqMessage);
    log.warn("Message sent to DLQ: partition={} offset={}",
            record.partition(), record.offset());
  }
}