package com.trade.stream;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class InstrumentPartitioner implements Partitioner {
    private final Map<String, Integer> instrumentToPartition = Map.of(
            "AAPL", 0,
            "GOOG", 1,
            "TSLA", 2,
            "MSFT", 3,
            "AMZN", 4
    );

    @Override
    public void configure(Map<String, ?> configs) {}


    @Override
    public int partition(String s, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String partitionKey = (String) key;
        return instrumentToPartition.getOrDefault(partitionKey, 0);
    }

    @Override
    public void close() {}
}