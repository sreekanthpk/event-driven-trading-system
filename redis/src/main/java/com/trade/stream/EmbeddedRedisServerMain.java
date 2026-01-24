package com.trade.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;

public class EmbeddedRedisServerMain {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedRedisServerMain.class);

  private RedisServer redisServer;
  private boolean stopped = false;

  public void start() throws Exception {
    if (redisServer != null && redisServer.isActive()) {
      log.info("Redis is already running");
      return;
    }

    redisServer = new RedisServerBuilder().port(6379).setting("maxheap 128M").build();
    redisServer.start();
    log.info("Embedded Redis started on port 6379");
  }

  public void stop() {
    if (!stopped && redisServer != null) {
      redisServer.stop();
      stopped = true;
      log.info("Embedded Redis stopped");
    }
  }

  public static void main(String[] args) {
    EmbeddedRedisServerMain server = new EmbeddedRedisServerMain();
    try {
      server.start();

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    log.info("Shutdown hook triggered");
                    server.stop();
                  }));

      log.info("Press ENTER to stop Redis...");
      System.in.read();

    } catch (Exception e) {
      log.error("Error starting Embedded Redis server", e);

    } finally {
      server.stop();
    }
  }
}
