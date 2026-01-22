package com.trade.stream;

import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;

public class EmbeddedRedisServerMain {

    private RedisServer redisServer;

    public void start() throws Exception {
        RedisServer redisServer = new RedisServerBuilder()
                .port(6379)
                .setting("maxheap 128M") // limit heap to 128 MB
                .build();
        redisServer.start();
        System.out.println("Embedded Redis started on port 6379");
    }

    public void stop() {
        if (redisServer != null) {
            redisServer.stop();
            System.out.println("Embedded Redis stopped");
        }
    }

    public static void main(String[] args) {
        EmbeddedRedisServerMain server = new EmbeddedRedisServerMain();
        try {
            server.start();
            System.out.println("Press ENTER to stop Redis...");
            System.in.read();  // wait for user input to stop
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            server.stop();
        }
    }
}
