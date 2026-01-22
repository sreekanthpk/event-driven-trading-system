package com.trade.stream;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import scala.Option;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Properties;

public class EmbeddedKafkaServer {
    public static void main(String[] args) throws Exception {

        // ---------- Start Zookeeper ----------
        int zkPort = 2181;
        File zkDir = new File("/tmp/zk");
        ZooKeeperServer zkServer = new ZooKeeperServer(zkDir, zkDir, 2000);
        ServerCnxnFactory zkFactory =
                ServerCnxnFactory.createFactory(new InetSocketAddress(zkPort), 1024);
        zkFactory.startup(zkServer);
        Thread.sleep(2000);

        // ---------- Kafka Broker Config ----------
        Properties props = new Properties();
        props.put("broker.id", "0");
        props.put("listeners", "PLAINTEXT://localhost:9092");
        props.put("log.dirs", "/tmp/kafka-logs");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("offsets.topic.replication.factor", "1");
        props.put("transaction.state.log.replication.factor", "1");
        props.put("transaction.state.log.min.isr", "1");

        KafkaConfig config = new KafkaConfig(props);

        // ---------- Start Kafka ----------
        KafkaServer kafkaServer = new KafkaServer(config, Time.SYSTEM, Option.empty(), false);
        kafkaServer.startup();

        System.out.println("Kafka started on localhost:9092");
    }
}