package com.trade.stream;

import io.vertx.core.Vertx;

public class WebSocketKafkaApp {

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();

    vertx.deployVerticle(new KafkaConsumerVerticle());
    vertx.deployVerticle(new WebSocketVerticle());
  }
}
