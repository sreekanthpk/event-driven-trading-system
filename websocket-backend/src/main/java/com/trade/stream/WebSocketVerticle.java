package com.trade.stream;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;

public class WebSocketVerticle extends AbstractVerticle {

  @Override
  public void start() {

    HttpServer server = vertx.createHttpServer();

    server.webSocketHandler(this::handleWebSocket).listen(8080);

    System.out.println("WebSocket server started on ws://localhost:8080/ws/inquiries");
  }

  private void handleWebSocket(ServerWebSocket ws) {

    if (!"/ws/inquiries".equals(ws.path())) {
      ws.reject();
      return;
    }

    System.out.println("Client connected: " + ws.remoteAddress());
    vertx.eventBus().publish("ui.client.connected", ws.textHandlerID());

    var consumer = vertx.eventBus().<byte[]>consumer("ui.updates");

    consumer.handler(
        msg -> {
          if (!ws.isClosed()) {
            byte[] data = msg.body();

            // Send as BINARY frame
            ws.writeBinaryMessage(Buffer.buffer(data));
          }
        });

    ws.closeHandler(
        v -> {
          vertx.eventBus().publish("ui.client.disconnected", ws.textHandlerID());
          consumer.unregister();
          System.out.println("Client disconnected");
        });
  }
}
