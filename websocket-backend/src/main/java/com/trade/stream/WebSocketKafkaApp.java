package com.trade.stream;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketKafkaApp {

  private static final Logger log = LoggerFactory.getLogger(WebSocketKafkaApp.class);

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    // Shutdown hook for cleanup
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Shutting down...");
                  vertx.close();
                }));

    // Deploy Kafka consumer first
    vertx.deployVerticle(
        new KafkaConsumerVerticle(),
        kafkaResult -> {
          if (kafkaResult.succeeded()) {
            log.info("Kafka consumer deployed: " + kafkaResult.result());

            // Then deploy WebSocket server
            vertx.deployVerticle(
                new WebSocketVerticle(),
                wsResult -> {
                  if (wsResult.succeeded()) {
                    log.info("WebSocket server deployed: " + wsResult.result());
                    log.info("Application started successfully!");
                  } else {
                    log.info("Failed to deploy WebSocket server");
                    wsResult.cause().printStackTrace();
                    System.exit(1);
                  }
                });
          } else {
            log.info("Failed to deploy Kafka consumer");
            kafkaResult.cause().printStackTrace();
            System.exit(1);
          }
        });
  }
}
