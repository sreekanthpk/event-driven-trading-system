package com.trade.stream;

import io.vertx.core.Vertx;

public class WebSocketKafkaApp {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    // Shutdown hook for cleanup
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutting down...");
      vertx.close();
    }));

    // Deploy Kafka consumer first
    vertx.deployVerticle(new KafkaConsumerVerticle(), kafkaResult -> {
      if (kafkaResult.succeeded()) {
        System.out.println("✓ Kafka consumer deployed: " + kafkaResult.result());

        // Then deploy WebSocket server
        vertx.deployVerticle(new WebSocketVerticle(), wsResult -> {
          if (wsResult.succeeded()) {
            System.out.println("✓ WebSocket server deployed: " + wsResult.result());
            System.out.println("Application started successfully!");
          } else {
            System.err.println("✗ Failed to deploy WebSocket server");
            wsResult.cause().printStackTrace();
            System.exit(1);
          }
        });
      } else {
        System.err.println("✗ Failed to deploy Kafka consumer");
        kafkaResult.cause().printStackTrace();
        System.exit(1);
      }
    });
  }
}