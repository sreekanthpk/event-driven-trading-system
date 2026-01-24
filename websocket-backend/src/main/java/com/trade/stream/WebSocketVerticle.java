package com.trade.stream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.trade.stream.common.Common;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class WebSocketVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(WebSocketVerticle.class);

    private static final int DEFAULT_PORT = 8080;
    private static final String DEFAULT_PATH = "/ws/inquiries";
    private static final int MAX_CONNECTIONS = 1000;
    private static final long PING_INTERVAL_MS = 30000; // 30 seconds
    private static final int MAX_WRITE_QUEUE_SIZE = 100;

    private final Map<String, ServerWebSocket> activeConnections = new ConcurrentHashMap<>();
    private final Map<String, Long> pingTimers = new ConcurrentHashMap<>();
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private MessageConsumer<byte[]> sharedConsumer;
    private HttpServer server;
    private String websocketPath;

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = config();

        int port = config.getInteger("websocket.port", DEFAULT_PORT);
        websocketPath = config.getString("websocket.path", DEFAULT_PATH);

        // Create HTTP server with WebSocket compression
        HttpServerOptions options = new HttpServerOptions()
                .setCompressionSupported(true)
                .setCompressionLevel(6)
                .setPerFrameWebSocketCompressionSupported(true)
                .setPerMessageWebSocketCompressionSupported(true)
                .setIdleTimeout(120); // 2 minutes

        server = vertx.createHttpServer(options);

        // Create shared event bus consumer
        sharedConsumer = vertx.eventBus().<byte[]>consumer("ui.updates");
        sharedConsumer.handler(this::broadcastToAllClients);

        // Start WebSocket server
        server.webSocketHandler(this::handleWebSocket).listen(port, result -> {
            if (result.succeeded()) {
                log.info("WebSocket server started on ws://localhost:{}{}", port, websocketPath);
                startPromise.complete();
            } else {
                log.error("Failed to start WebSocket server", result.cause());
                startPromise.fail(result.cause());
            }
        });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        log.info("Stopping WebSocket server. Active connections: {}", activeConnections.size());

        // Cancel all ping timers
        pingTimers.values().forEach(vertx::cancelTimer);
        pingTimers.clear();

        // Close all active connections
        activeConnections.values().forEach(ws -> {
            try {
                if (!ws.isClosed()) {
                    ws.close((short) 1001, "Server shutting down");
                }
            } catch (Exception e) {
                log.error("Error closing WebSocket connection", e);
            }
        });

        activeConnections.clear();

        // Unregister shared consumer
        if (sharedConsumer != null) {
            sharedConsumer.unregister(result -> {
                if (result.succeeded()) {
                    log.info("Event bus consumer unregistered successfully");

                    // Close HTTP server
                    if (server != null) {
                        server.close(closeResult -> {
                            if (closeResult.succeeded()) {
                                log.info("WebSocket server stopped successfully");
                                stopPromise.complete();
                            } else {
                                log.error("Error closing HTTP server", closeResult.cause());
                                stopPromise.fail(closeResult.cause());
                            }
                        });
                    } else {
                        stopPromise.complete();
                    }
                } else {
                    log.error("Failed to unregister event bus consumer", result.cause());
                    stopPromise.fail(result.cause());
                }
            });
        } else {
            stopPromise.complete();
        }
    }

    private void handleWebSocket(ServerWebSocket ws) {
        // Validate path
        if (!websocketPath.equals(ws.path())) {
            log.debug("Rejected connection to invalid path: {}", ws.path());
            ws.reject(404);
            return;
        }

        // Check connection limit
        int currentConnections = connectionCount.incrementAndGet();
        if (currentConnections > MAX_CONNECTIONS) {
            log.warn("Connection limit reached ({}/{}). Rejecting connection from {}",
                    currentConnections, MAX_CONNECTIONS, ws.remoteAddress());
            connectionCount.decrementAndGet();
            ws.reject(503);
            return;
        }

        /*
        Disabling authorization for now
         */
        /*String token = ws.headers().get("Authorization");
        if (!isValidToken(token)) {
            log.warn("Unauthorized WebSocket connection attempt from {}", ws.remoteAddress());
            connectionCount.decrementAndGet();
            ws.reject(401);
            return;
        }*/

        String clientId = ws.textHandlerID();

        try {
            log.info("Client connected ({}/{}): clientId={}, address={}",
                    currentConnections, MAX_CONNECTIONS, clientId, ws.remoteAddress());

            // Configure WebSocket
            ws.setWriteQueueMaxSize(MAX_WRITE_QUEUE_SIZE);

            // Track connection
            activeConnections.put(clientId, ws);
            vertx.eventBus().publish("ui.client.connected", clientId);

            // Set up periodic ping
            long pingTimerId = vertx.setPeriodic(PING_INTERVAL_MS, id -> {
                sendPing(clientId, ws, id);
            });
            pingTimers.put(clientId, pingTimerId);

            // Handle pong responses
            ws.pongHandler(buffer -> {
                log.trace("Received pong from client {}", clientId);
            });

            // Handle incoming text messages (if needed)
            ws.textMessageHandler(text -> {
                log.debug("Received text message from {}: {}", clientId, text);
                handleClientMessage(clientId, text);
            });

            // Handle binary messages (if needed)
            ws.binaryMessageHandler(buffer -> {
                log.debug("Received binary message from {}: {} bytes", clientId, buffer.length());
            });

            // Handle drain (backpressure)
            ws.drainHandler(v -> {
                log.trace("Write queue drained for client {}", clientId);
            });

            // Handle errors
            ws.exceptionHandler(error -> {
                log.error("WebSocket error for client {}: {}", clientId, error.getMessage());
                cleanupConnection(clientId, ws);
            });

            // Handle close
            ws.closeHandler(v -> {
                log.info("Client disconnected: clientId={}", clientId);
                cleanupConnection(clientId, ws);
            });

        } catch (Exception e) {
            log.error("Error handling WebSocket connection from {}", ws.remoteAddress(), e);
            connectionCount.decrementAndGet();

            if (!ws.isClosed()) {
                ws.close((short) 1011, "Internal server error");
            }
        }
    }

    private void sendPing(String clientId, ServerWebSocket ws, long timerId) {
        if (!ws.isClosed()) {
            ws.writePing(Buffer.buffer("ping"), result -> {
                if (result.failed()) {
                    log.warn("Failed to send ping to client {}: {}",
                            clientId, result.cause().getMessage());
                    vertx.cancelTimer(timerId);
                    cleanupConnection(clientId, ws);
                }
            });
        } else {
            vertx.cancelTimer(timerId);
        }
    }

    private void handleClientMessage(String clientId, String message) {
        // Handle client commands (e.g., subscribe to specific symbols)
        // This is a placeholder for future enhancements
        log.debug("Processing message from client {}: {}", clientId, message);
    }

    private void broadcastToAllClients(Message<byte[]> msg) {
        byte[] data = msg.body();

        if (activeConnections.isEmpty()) {
            log.trace("No active connections. Skipping broadcast.");
            return;
        }

        try {
            // Parse inquiry for potential filtering
            Common.Inquiry inquiry = Common.Inquiry.parseFrom(data);

            Buffer buffer = Buffer.buffer(data);
            int successCount = 0;
            int failureCount = 0;

            for (Map.Entry<String, ServerWebSocket> entry : activeConnections.entrySet()) {
                String clientId = entry.getKey();
                ServerWebSocket ws = entry.getValue();

                if (!ws.isClosed() && shouldSendToClient(clientId, inquiry)) {
                    if (sendToClient(ws, buffer)) {
                        successCount++;
                    } else {
                        failureCount++;
                    }
                }
            }

            log.debug("Broadcast complete: inquiryId={}, sent={}, failed={}",
                    inquiry.getInquiryId(), successCount, failureCount);

        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse inquiry protobuf for broadcasting", e);
        }
    }

    private boolean sendToClient(ServerWebSocket ws, Buffer buffer) {
        if (ws.isClosed()) {
            return false;
        }

        if (ws.writeQueueFull()) {
            log.warn("Write queue full for client {}. Dropping message.", ws.textHandlerID());
            return false;
        }

        try {
            ws.writeBinaryMessage(buffer, result -> {
                if (result.failed()) {
                    log.warn("Failed to send message to client {}: {}",
                            ws.textHandlerID(), result.cause().getMessage());
                }
            });
            return true;
        } catch (Exception e) {
            log.error("Exception sending message to client {}: {}",
                    ws.textHandlerID(), e.getMessage());
            return false;
        }
    }

    private boolean shouldSendToClient(String clientId, Common.Inquiry inquiry) {
        // Implement filtering logic here
        // Example: only send to clients subscribed to specific symbols
        // For now, broadcast to all clients
        return true;
    }

    private void cleanupConnection(String clientId, ServerWebSocket ws) {
        // Remove from active connections
        activeConnections.remove(clientId);

        // Cancel ping timer
        Long timerId = pingTimers.remove(clientId);
        if (timerId != null) {
            vertx.cancelTimer(timerId);
        }

        // Decrement connection count
        connectionCount.decrementAndGet();

        // Notify other components
        vertx.eventBus().publish("ui.client.disconnected", clientId);

        // Close WebSocket if still open
        if (!ws.isClosed()) {
            try {
                ws.close();
            } catch (Exception e) {
                log.error("Error closing WebSocket for client {}", clientId, e);
            }
        }

        log.debug("Connection cleaned up: clientId={}, remaining={}",
                clientId, activeConnections.size());
    }
}