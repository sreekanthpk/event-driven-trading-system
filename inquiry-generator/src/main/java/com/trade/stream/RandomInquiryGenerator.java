package com.trade.stream;
import com.trade.stream.common.Common;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class RandomInquiryGenerator {

    private static final AtomicLong SEQUENCE = new AtomicLong(1);

    private static final List<String> INSTRUMENTS =
            List.of("AAPL", "MSFT", "GOOG", "AMZN", "TSLA");

    private static final List<Common.Enums.Side> SIDES =
            List.of(Common.Enums.Side.BUY, Common.Enums.Side.SELL);

    private static final List<String> BOOK =
            List.of("BOOK1", "BOOK2", "BOOK3");

    private static final List<String> COUNTERPARTY =
            List.of("cpt1", "cpt2", "cpt3");

    private final Producer<Long, byte[]> producer;
    private final String topic;
    private final Random random = new Random();

    public RandomInquiryGenerator(Producer<Long, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public void sendRandomInquiry() {
        Common.Inquiry inquiry = Common.Inquiry.newBuilder()
                .setInquiryId(SEQUENCE.getAndIncrement())
                .setInstrumentId(randomFrom(INSTRUMENTS))
                .setSide(randomFrom(SIDES))
                .setQuantity((random.nextInt(50) + 1) * 100L)
                .setPrice(random.nextLong(100,10000))
                .setBookId(randomFrom(BOOK))
                .setEventTime(System.currentTimeMillis())
                .setCounterparty(randomFrom(COUNTERPARTY))
                .setStatus(Common.Enums.Status.NEW)
                .setVersion(1)
                .build();

        ProducerRecord<Long, byte[]> record =
                new ProducerRecord<>(
                        topic,
                        inquiry.getInquiryId(),
                        inquiry.toByteArray()
                );

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.printf(
                        "Sent inquiry %s to %s partition=%d offset=%d%n",
                        inquiry.getInquiryId(),
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                );
            }
        });
    }

    private <T> T randomFrom(List<T> list) {
        return list.get(random.nextInt(list.size()));
    }
}
