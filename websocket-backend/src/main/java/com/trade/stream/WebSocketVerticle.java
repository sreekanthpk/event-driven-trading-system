package com.trade.stream;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;

import java.nio.charset.StandardCharsets;

public class WebSocketVerticle extends AbstractVerticle {

    @Override
    public void start() {

        HttpServer server = vertx.createHttpServer();

        server.webSocketHandler(this::handleWebSocket)
                .listen(8080);

        System.out.println("WebSocket server started on ws://localhost:8080/ws/inquiries");
    }

    private void handleWebSocket(ServerWebSocket ws) {

        if (!"/ws/inquiries".equals(ws.path())) {
            ws.reject();
            return;
        }

        System.out.println("Client connected: " + ws.remoteAddress());

        var consumer = vertx.eventBus()
                .<byte[]>consumer("ui.updates");

        consumer.handler(msg -> {
            if (!ws.isClosed()) {
                byte[] data = msg.body();

                // Send as BINARY frame
                ws.writeBinaryMessage(Buffer.buffer(data));
            }
        });

        ws.closeHandler(v -> {
            consumer.unregister();
            System.out.println("Client disconnected");
        });
    }
}
