package com.trade.stream;

import static org.junit.jupiter.api.Assertions.*;

import com.trade.stream.common.Common;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

public class PositionProcessorTest {

  private RedisServer redisServer;
  private Jedis jedis;
  private MockProducer<Long, byte[]> mockProducer;
  private PositionProcessor processor;

  @BeforeEach
  void setUp() throws IOException {
    // Start embedded Redis
    redisServer = new RedisServer(6380);
    redisServer.start();
    jedis = new Jedis("localhost", 6380);

    // Create mock Kafka producer
    mockProducer = new MockProducer<>(true, new LongSerializer(), new ByteArraySerializer());

    // Create processor
    processor = new PositionProcessor(jedis, mockProducer, "inquiry-topic");
  }

  @AfterEach
  void tearDown() {
    if (jedis != null) {
      jedis.flushAll();
      jedis.close();
    }
    if (redisServer != null) {
      redisServer.stop();
    }
    if (mockProducer != null) {
      mockProducer.close();
    }
  }

  @Test
  void shouldCalculatePositionForBuy() {
    // Test pure business logic
    long position = PositionProcessor.calculatePosition(100, 50, Common.Enums.Side.BUY);
    assertEquals(5000, position);
  }

  @Test
  void shouldCalculatePositionForSell() {
    // Sell should be negative
    long position = PositionProcessor.calculatePosition(100, 50, Common.Enums.Side.SELL);
    assertEquals(-5000, position);
  }

  @Test
  void shouldBuildPositionKey() {
    String key = PositionProcessor.buildPositionKey("AAPL", "BOOK1");
    assertEquals("position:AAPL:BOOK1", key);
  }

  @Test
  void shouldEnrichNewInquiryWithPosition() throws Exception {
    // Setup: Set initial position in Redis
    String key = "position:AAPL:BOOK1";
    jedis.hset(key, "position", "1000");

    // Create NEW inquiry
    Common.Inquiry inquiry =
        Common.Inquiry.newBuilder()
            .setInquiryId(1L)
            .setInstrumentId("AAPL")
            .setBookId("BOOK1")
            .setSide(Common.Enums.Side.BUY)
            .setQuantity(100)
            .setPrice(150)
            .setStatus(Common.Enums.Status.NEW)
            .setVersion(1)
            .build();

    // Process
    processor.processInquiry(inquiry);

    // Verify: Message was sent to Kafka
    List<ProducerRecord<Long, byte[]>> records = mockProducer.history();
    assertEquals(1, records.size());

    // Parse the sent inquiry
    Common.Inquiry sentInquiry = Common.Inquiry.parseFrom(records.get(0).value());

    assertEquals(2, sentInquiry.getVersion(), "Version should be incremented");
    assertEquals(1000, sentInquiry.getPosition(), "Position should be enriched");
    assertEquals(Common.Enums.Status.POSITION_ENRICHED, sentInquiry.getStatus());
  }

  @Test
  void shouldUpdatePositionForDoneBuy() throws Exception {
    // Create DONE inquiry for BUY
    Common.Inquiry inquiry =
        Common.Inquiry.newBuilder()
            .setInquiryId(2L)
            .setInstrumentId("MSFT")
            .setBookId("BOOK2")
            .setSide(Common.Enums.Side.BUY)
            .setQuantity(50)
            .setPrice(200)
            .setStatus(Common.Enums.Status.DONE)
            .setVersion(1)
            .build();

    // Process
    processor.processInquiry(inquiry);

    // Verify: Position updated in Redis (50 * 200 = 10000)
    String key = "position:MSFT:BOOK2";
    String position = jedis.hget(key, "position");
    assertEquals("10000", position);
  }

  @Test
  void shouldUpdatePositionForDoneSell() throws Exception {
    // Create DONE inquiry for SELL
    Common.Inquiry inquiry =
        Common.Inquiry.newBuilder()
            .setInquiryId(3L)
            .setInstrumentId("GOOG")
            .setBookId("BOOK3")
            .setSide(Common.Enums.Side.SELL)
            .setQuantity(30)
            .setPrice(100)
            .setStatus(Common.Enums.Status.DONE)
            .setVersion(1)
            .build();

    // Process
    processor.processInquiry(inquiry);

    // Verify: Position is negative for SELL (-30 * 100 = -3000)
    String key = "position:GOOG:BOOK3";
    String position = jedis.hget(key, "position");
    assertEquals("-3000", position);
  }

  @Test
  void shouldAccumulatePositions() throws Exception {
    // Process multiple DONE inquiries for same instrument
    String instrumentId = "TSLA";
    String bookId = "BOOK1";

    // BUY 100 @ 500 = +50000
    Common.Inquiry buy1 =
        Common.Inquiry.newBuilder()
            .setInquiryId(1L)
            .setInstrumentId(instrumentId)
            .setBookId(bookId)
            .setSide(Common.Enums.Side.BUY)
            .setQuantity(100)
            .setPrice(500)
            .setStatus(Common.Enums.Status.DONE)
            .setVersion(1)
            .build();

    // SELL 50 @ 500 = -25000
    Common.Inquiry sell1 =
        Common.Inquiry.newBuilder()
            .setInquiryId(2L)
            .setInstrumentId(instrumentId)
            .setBookId(bookId)
            .setSide(Common.Enums.Side.SELL)
            .setQuantity(50)
            .setPrice(500)
            .setStatus(Common.Enums.Status.DONE)
            .setVersion(1)
            .build();

    // Process both
    processor.processInquiry(buy1);
    processor.processInquiry(sell1);

    // Verify: Net position = 50000 - 25000 = 25000
    String key = "position:TSLA:BOOK1";
    String position = jedis.hget(key, "position");
    assertEquals("25000", position);
  }

  @Test
  void shouldHandleDifferentBooks() throws Exception {
    // Same instrument, different books
    Common.Inquiry book1 =
        Common.Inquiry.newBuilder()
            .setInquiryId(1L)
            .setInstrumentId("AAPL")
            .setBookId("BOOK1")
            .setSide(Common.Enums.Side.BUY)
            .setQuantity(100)
            .setPrice(150)
            .setStatus(Common.Enums.Status.DONE)
            .setVersion(1)
            .build();

    Common.Inquiry book2 =
        Common.Inquiry.newBuilder()
            .setInquiryId(2L)
            .setInstrumentId("AAPL")
            .setBookId("BOOK2")
            .setSide(Common.Enums.Side.BUY)
            .setQuantity(200)
            .setPrice(150)
            .setStatus(Common.Enums.Status.DONE)
            .setVersion(1)
            .build();

    processor.processInquiry(book1);
    processor.processInquiry(book2);

    // Verify: Different positions per book
    assertEquals("15000", jedis.hget("position:AAPL:BOOK1", "position"));
    assertEquals("30000", jedis.hget("position:AAPL:BOOK2", "position"));
  }

  @Test
  void shouldIgnoreOtherStatuses() throws Exception {
    // Create inquiry with POSITION_ENRICHED status (should be ignored)
    Common.Inquiry inquiry =
        Common.Inquiry.newBuilder()
            .setInquiryId(1L)
            .setInstrumentId("AAPL")
            .setBookId("BOOK1")
            .setSide(Common.Enums.Side.BUY)
            .setQuantity(100)
            .setPrice(150)
            .setStatus(Common.Enums.Status.POSITION_ENRICHED)
            .setVersion(1)
            .build();

    processor.processInquiry(inquiry);

    // Verify: No Kafka message sent
    assertTrue(mockProducer.history().isEmpty());

    // Verify: No Redis update
    String key = "position:AAPL:BOOK1";
    assertNull(jedis.hget(key, "position"));
  }

  @Test
  void shouldHandleZeroPosition() throws Exception {
    // NEW inquiry when position is 0
    Common.Inquiry inquiry =
        Common.Inquiry.newBuilder()
            .setInquiryId(1L)
            .setInstrumentId("NEW_STOCK")
            .setBookId("BOOK1")
            .setSide(Common.Enums.Side.BUY)
            .setQuantity(100)
            .setPrice(50)
            .setStatus(Common.Enums.Status.NEW)
            .setVersion(1)
            .build();

    processor.processInquiry(inquiry);

    // Verify: Position is 0 (first time)
    List<ProducerRecord<Long, byte[]>> records = mockProducer.history();
    Common.Inquiry sentInquiry = Common.Inquiry.parseFrom(records.get(0).value());
    assertEquals(0, sentInquiry.getPosition());
  }

  @Test
  void shouldHandleMultipleNewInquiries() throws Exception {
    // Set initial position
    jedis.hset("position:AAPL:BOOK1", "position", "5000");

    // Process 3 NEW inquiries
    for (int i = 1; i <= 3; i++) {
      Common.Inquiry inquiry =
          Common.Inquiry.newBuilder()
              .setInquiryId(i)
              .setInstrumentId("AAPL")
              .setBookId("BOOK1")
              .setSide(Common.Enums.Side.BUY)
              .setQuantity(100)
              .setPrice(50)
              .setStatus(Common.Enums.Status.NEW)
              .setVersion(1)
              .build();

      processor.processInquiry(inquiry);
    }

    // Verify: 3 messages sent
    assertEquals(3, mockProducer.history().size());

    // All should have same position (5000) since we didn't update it
    for (ProducerRecord<Long, byte[]> record : mockProducer.history()) {
      Common.Inquiry sent = Common.Inquiry.parseFrom(record.value());
      assertEquals(5000, sent.getPosition());
    }
  }

  @Test
  void shouldIncrementVersionForNewInquiry() throws Exception {
    Common.Inquiry inquiry =
        Common.Inquiry.newBuilder()
            .setInquiryId(1L)
            .setInstrumentId("AAPL")
            .setBookId("BOOK1")
            .setSide(Common.Enums.Side.BUY)
            .setQuantity(100)
            .setPrice(50)
            .setStatus(Common.Enums.Status.NEW)
            .setVersion(5) // Start at version 5
            .build();

    processor.processInquiry(inquiry);

    // Verify: Version incremented
    Common.Inquiry sent = Common.Inquiry.parseFrom(mockProducer.history().get(0).value());
    assertEquals(6, sent.getVersion());
  }
}
