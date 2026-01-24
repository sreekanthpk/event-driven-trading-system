package com.trade.stream;

import com.trade.stream.common.Common;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomInquiryGenerator {

  private static final Logger log = LoggerFactory.getLogger(RandomInquiryGenerator.class);
  private static final AtomicLong SEQUENCE = new AtomicLong(1);

  private static final List<String> INSTRUMENTS = List.of("AAPL", "MSFT", "GOOG", "AMZN", "TSLA");
  private static final List<Common.Enums.Side> SIDES =
      List.of(Common.Enums.Side.BUY, Common.Enums.Side.SELL);
  private static final List<String> BOOK = List.of("BOOK1", "BOOK2", "BOOK3");
  private static final List<String> COUNTERPARTY = List.of("cpt1", "cpt2", "cpt3");

  private final Producer<Long, byte[]> producer;
  private final String topic;

  public RandomInquiryGenerator(Producer<Long, byte[]> producer, String topic) {
    this.producer = producer;
    this.topic = topic;
  }

  public void sendRandomInquiry() {
    ThreadLocalRandom random = ThreadLocalRandom.current();

    Common.Inquiry inquiry =
        Common.Inquiry.newBuilder()
            .setInquiryId(SEQUENCE.getAndIncrement())
            .setInstrumentId(randomFrom(INSTRUMENTS, random))
            .setSide(randomFrom(SIDES, random))
            .setQuantity((random.nextInt(50) + 1) * 100L)
            .setPrice(random.nextLong(100, 10000))
            .setBookId(randomFrom(BOOK, random))
            .setEventTime(System.currentTimeMillis())
            .setCounterparty(randomFrom(COUNTERPARTY, random))
            .setStatus(Common.Enums.Status.NEW)
            .setVersion(1)
            .build();

    ProducerRecord<Long, byte[]> record =
        new ProducerRecord<>(topic, inquiry.getInquiryId(), inquiry.toByteArray());

    producer.send(
        record,
        (metadata, exception) -> {
          if (exception != null) {
            log.error(
                "Failed to send inquiry {}: {}",
                inquiry.getInquiryId(),
                exception.getMessage(),
                exception);
          } else {
            log.info(
                "Sent inquiry {} to {} partition={} offset={}",
                inquiry.getInquiryId(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset());
          }
        });
  }

  private <T> T randomFrom(List<T> list, ThreadLocalRandom random) {
    return list.get(random.nextInt(list.size()));
  }
}
