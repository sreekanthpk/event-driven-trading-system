package com.trade.stream;

import static org.junit.Assert.assertEquals;

import com.trade.stream.common.Common;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.Assert;

public class WebSocketIntegrationTest {
  static List<Common.Inquiry> inquiries = new ArrayList<>();

  public static void main(String[] args) throws InterruptedException {

    Vertx vertx = Vertx.vertx();
    CountDownLatch latch = new CountDownLatch(1);

    vertx
        .createHttpClient()
        .webSocket(8080, "localhost", "/ws/inquiries")
        .onSuccess(ws -> handleWebSocket(ws, latch))
        .onFailure(Throwable::printStackTrace);
  }

  private static void handleWebSocket(WebSocket ws, CountDownLatch latch) {

    System.out.println("Connected to WebSocket server");

    ws.binaryMessageHandler(Buffer::getBytes);

    ws.binaryMessageHandler(
        buffer -> {
          try {
            Common.Inquiry inquiry = Common.Inquiry.parseFrom(buffer.getBytes());
            verifyInquiries(inquiry);
          } catch (Exception e) {
            System.out.println(e.getMessage());
          }
        });

    ws.closeHandler(v -> System.out.println("WebSocket closed"));
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
