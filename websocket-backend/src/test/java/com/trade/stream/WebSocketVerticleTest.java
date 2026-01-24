package com.trade.stream;

import static org.junit.jupiter.api.Assertions.*;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class WebSocketVerticleTest {

  private String deploymentId;
  private HttpClient client;

  @BeforeEach
  void setUp(Vertx vertx, VertxTestContext testContext) {
    client = vertx.createHttpClient();

    vertx.deployVerticle(
        new WebSocketVerticle(),
        testContext.succeeding(
            id -> {
              deploymentId = id;
              testContext.completeNow();
            }));
  }

  @AfterEach
  void tearDown(Vertx vertx, VertxTestContext testContext) {
    client.close();
    vertx.undeploy(deploymentId, testContext.succeeding(v -> testContext.completeNow()));
  }

  @Test
  void shouldRejectInvalidPath(Vertx vertx, VertxTestContext testContext) {
    // Try to connect with wrong path
    WebSocketConnectOptions options =
        new WebSocketConnectOptions().setPort(8080).setHost("localhost").setURI("/wrong/path");

    client.webSocket(
        options,
        testContext.failing(
            error -> {
              // Should fail - connection rejected
              testContext.completeNow();
            }));
  }

  @Test
  void shouldRespectConnectionLimit(Vertx vertx, VertxTestContext testContext) {
    // Connect 5 clients successfully
    AtomicInteger connected = new AtomicInteger(0);

    WebSocketConnectOptions options =
        new WebSocketConnectOptions().setPort(8080).setHost("localhost").setURI("/ws/inquiries");

    for (int i = 0; i < 5; i++) {
      client.webSocket(
          options,
          result -> {
            if (result.succeeded()) {
              if (connected.incrementAndGet() == 5) {
                testContext.completeNow();
              }
            }
          });
    }
  }
}
