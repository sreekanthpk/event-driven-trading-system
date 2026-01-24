package com.trade.stream;

import static org.junit.Assert.assertEquals;

import com.trade.stream.common.Common;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketIntegrationTest {
  private static final Logger log = LoggerFactory.getLogger(WebSocketIntegrationTest.class);

  static List<Common.Inquiry> inquiries = new ArrayList<>();

  public static void main(String[] args) throws InterruptedException {

    Vertx vertx = Vertx.vertx();

    vertx
        .createHttpClient()
        .webSocket(8080, "localhost", "/ws/inquiries?clientId=test")
        .onSuccess(WebSocketIntegrationTest::handleWebSocket)
        .onFailure(Throwable::printStackTrace);
  }

  private static void handleWebSocket(WebSocket ws) {

    log.info("Connected to WebSocket server");

    ws.binaryMessageHandler(Buffer::getBytes);

    ws.binaryMessageHandler(
        buffer -> {
          try {
            Common.Inquiry inquiry = Common.Inquiry.parseFrom(buffer.getBytes());
            verifyInquiries(inquiry);
          } catch (Exception e) {
            log.error("Error processing inquiry", e);
          }
        });

    ws.closeHandler(v -> log.info("WebSocket closed"));
  }

  private static void verifyInquiries(Common.Inquiry inquiry) {
    if (inquiry.getStatus() != Common.Enums.Status.DONE
        && inquiry.getStatus() != Common.Enums.Status.NOT_DONE) return;
    inquiries.add(inquiry);
    System.out.println("Inquiry received" + inquiry.getInquiryId());
    if (inquiries.size() == 20) {
      verifyPosition();
      verifySequence();
      System.out.println("Integration test completed Successfully");
    }
  }

  private static void verifySequence() {
    for (int i = 1; i < inquiries.size(); i++) {
      Assert.assertEquals(inquiries.get(i - 1).getInquiryId() + 1, inquiries.get(i).getInquiryId());
    }
  }

  private static void verifyPosition() {
    long position = 0;
    for (int i = 1; i < inquiries.size(); i++) {
      if (inquiries.get(i - 1).getStatus() == Common.Enums.Status.DONE) {
        long inquiryPos = inquiries.get(i).getPrice() * inquiries.get(i).getQuantity();
        if (inquiries.get(i - 1).getSide() == Common.Enums.Side.SELL) {
          inquiryPos = -inquiryPos;
        }
        position = position + inquiryPos;
      }

      assertEquals(position, inquiries.get(i).getPosition());
    }
  }
}
