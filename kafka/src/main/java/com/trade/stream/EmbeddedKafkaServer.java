package com.trade.stream;

import static com.trade.stream.CommonConstants.KAFKA_BOOTSTRAP;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class EmbeddedKafkaServer {
  private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaServer.class);

  private static final String ZK_DIR = "/tmp/zk";
  private static final String KAFKA_LOG_DIR = "/tmp/kafka-logs";

  public static void main(String[] args) throws Exception {

    // ---------- Clean previous state ----------
    deleteDir(ZK_DIR);
    deleteDir(KAFKA_LOG_DIR);

    // ---------- Start ZooKeeper ----------
    int zkPort = 2181;
    File zkDir = new File(ZK_DIR);

    ZooKeeperServer zkServer = new ZooKeeperServer(zkDir, zkDir, 2000);

    ServerCnxnFactory zkFactory =
        ServerCnxnFactory.createFactory(new InetSocketAddress(zkPort), 1024);

    zkFactory.startup(zkServer);
    Thread.sleep(2000);

    // ---------- Kafka Broker Config ----------
    Properties props = new Properties();
    props.put("broker.id", "0");
    props.put("listeners", "PLAINTEXT://" + KAFKA_BOOTSTRAP);
    props.put("log.dirs", KAFKA_LOG_DIR);
    props.put("zookeeper.connect", "localhost:2181");
    props.put("offsets.topic.replication.factor", "1");
    props.put("transaction.state.log.replication.factor", "1");
    props.put("transaction.state.log.min.isr", "1");
    props.put("auto.create.topics.enable", "true");

    KafkaConfig config = new KafkaConfig(props);

    // ---------- Start Kafka ----------
    KafkaServer kafkaServer = new KafkaServer(config, Time.SYSTEM, Option.empty(), false);

    kafkaServer.startup();

    log.info("Kafka started clean on " + KAFKA_BOOTSTRAP);
  }

  private static void deleteDir(String path) throws Exception {
    File dir = new File(path);
    if (!dir.exists()) return;

    Files.walk(dir.toPath())
        .sorted(Comparator.reverseOrder())
        .forEach(
            p -> {
              try {
                Files.delete(p);
              } catch (Exception e) {
                throw new RuntimeException("Failed to delete " + p, e);
              }
            });
  }
}
