package org.gnuhpc.bigdata.utils;

import kafka.admin.AdminClient;
import kafka.admin.AdminUtils;
import kafka.coordinator.GroupOverview;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gnuhpc.bigdata.CollectionConvertor;
import org.gnuhpc.bigdata.config.KafkaConfig;
import org.gnuhpc.bigdata.config.ZookeeperConfig;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Properties;

/**
 * Created by gnuhpc on 2017/7/12.
 */
@Log4j
@Getter
@Setter
@Configuration
public class KafkaUtils {
    @Autowired
    private KafkaConfig kafkaConfig;
    @Autowired
    private ZookeeperConfig zookeeperConfig;

    private AdminClient kafkaAdminClient;

    private KafkaProducer producer;
    private Properties prop;

    private static final String DEFAULTCP = "kafka-rest-consumergroup";

    public void init(){
        prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaConfig.getBrokers());
        prop.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(prop);
        log.info("Kafka initing...");

        kafkaAdminClient = AdminClient.create(prop);
    }

  
    public void destroy(){
        log.info("Kafka destorying...");
    }

    public KafkaConsumer createNewConsumer(){
        return createNewConsumer(DEFAULTCP);
    }

    public KafkaConsumer createNewConsumer(String consumerGroup) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConfig().getBrokers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,"100000000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());

        return new KafkaConsumer(properties);
    }

    public Node getLeader(String topic, int partitionId) {
        KafkaConsumer consumer = createNewConsumer(DEFAULTCP);
        List<PartitionInfo> tmList = consumer.partitionsFor(topic);

        PartitionInfo partitionInfo = tmList.stream().filter(pi -> pi.partition() == partitionId).findFirst().get();
        consumer.close();
        return partitionInfo.leader();
    }

    public AdminClient createAdminClient() {
        return AdminClient.createSimplePlaintext(getKafkaConfig().getBrokers());
    }
}
