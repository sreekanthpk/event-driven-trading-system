package com.trade.stream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.trade.stream.common.Common;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import redis.clients.jedis.Jedis;

public class PositionProcessor {

  private final Jedis redis;
  private final Producer<Long, byte[]> producer;
  private final String topic;

  public PositionProcessor(Jedis redis, Producer<Long, byte[]> producer, String topic) {
    this.redis = redis;
    this.producer = producer;
    this.topic = topic;
  }

  public void processInquiry(Common.Inquiry inquiry) throws InvalidProtocolBufferException {
    switch (inquiry.getStatus()) {
      case NEW:
        handleNewInquiry(inquiry);
        break;
      case DONE:
        handleDoneInquiry(inquiry);
        break;
      default:
        // ignore
    }
  }

  private void handleNewInquiry(Common.Inquiry inquiry) throws InvalidProtocolBufferException {
    String key = buildPositionKey(inquiry.getInstrumentId(), inquiry.getBookId());

    // Get current position
    long position = redis.hincrBy(key, "position", 0);

    // Create updated inquiry
    Common.Inquiry updatedInquiry =
        inquiry.toBuilder()
            .setVersion(inquiry.getVersion() + 1)
            .setPosition(position)
            .setStatus(Common.Enums.Status.POSITION_ENRICHED)
            .build();

    // Publish updated inquiry
    producer.send(new ProducerRecord<>(topic, updatedInquiry.toByteArray()));
  }

  private void handleDoneInquiry(Common.Inquiry inquiry) {
    String key = buildPositionKey(inquiry.getInstrumentId(), inquiry.getBookId());

    long position = calculatePosition(inquiry.getQuantity(), inquiry.getPrice(), inquiry.getSide());
    redis.hincrBy(key, "position", position);
  }

  // Testable business logic
  public static long calculatePosition(long quantity, long price, Common.Enums.Side side) {
    long position = quantity * price;
    if (side == Common.Enums.Side.SELL) {
      position = -position;
    }
    return position;
  }

  public static String buildPositionKey(String instrumentId, String bookId) {
    return "position:" + instrumentId + ":" + bookId;
  }
}
