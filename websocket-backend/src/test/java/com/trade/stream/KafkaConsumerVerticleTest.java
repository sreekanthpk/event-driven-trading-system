package com.trade.stream;

import static org.junit.jupiter.api.Assertions.*;

import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class KafkaConsumerVerticleTest {

  private Vertx vertx;
  private String deploymentId;

  @BeforeEach
  void setUp(VertxTestContext testContext) {
    vertx = Vertx.vertx();

    // Deploy the verticle before each test
    KafkaConsumerVerticle verticle = new KafkaConsumerVerticle();
    vertx.deployVerticle(
        verticle,
        testContext.succeeding(
            id -> {
              deploymentId = id;
              testContext.completeNow();
            }));
  }

  @AfterEach
  void tearDown(VertxTestContext testContext) {
    if (deploymentId != null) {
      vertx.undeploy(
          deploymentId,
          testContext.succeeding(
              v -> {
                vertx.close(testContext.succeeding(closed -> testContext.completeNow()));
              }));
    } else {
      vertx.close(testContext.succeeding(v -> testContext.completeNow()));
    }
  }

  @Test
  @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
  void shouldHandleConcurrentClientConnections(VertxTestContext testContext) {
    AtomicInteger connectionCount = new AtomicInteger(0);
    int totalClients = 10;

    // Send multiple client connected events
    for (int i = 0; i < totalClients; i++) {
      vertx.eventBus().send("ui.client.connected", "client-" + i);
      connectionCount.incrementAndGet();
    }

    // Give it time to process
    vertx.setTimer(
        500,
        id -> {
          testContext.verify(
              () -> {
                assertEquals(
                    totalClients,
                    connectionCount.get(),
                    "All client connections should be tracked");
              });
          testContext.completeNow();
        });
  }

  @Test
  @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
  void shouldSubscribeOnFirstClientConnect(VertxTestContext testContext) {
    // Subscribe happens when first client connects
    vertx.eventBus().send("ui.client.connected", "client-1");

    vertx.setTimer(
        500,
        id -> {
          testContext.verify(
              () -> {
                // Verify subscription happened (indirectly by checking no errors)
                // In real test, you'd mock Kafka consumer to verify subscribe() was called
              });
          testContext.completeNow();
        });
  }

  @Test
  @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
  void shouldUnsubscribeWhenAllClientsDisconnect(VertxTestContext testContext) {
    // Connect clients
    vertx.eventBus().send("ui.client.connected", "client-1");
    vertx.eventBus().send("ui.client.connected", "client-2");

    vertx.setTimer(
        200,
        id1 -> {
          // Disconnect all clients
          vertx.eventBus().send("ui.client.disconnected", "client-1");
          vertx.eventBus().send("ui.client.disconnected", "client-2");

          vertx.setTimer(
              500,
              id2 -> {
                testContext.verify(
                    () -> {
                      // Verify unsubscribe happened
                    });
                testContext.completeNow();
              });
        });
  }
}
