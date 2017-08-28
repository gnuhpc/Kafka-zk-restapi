package org.gnuhpc.bigdata.utils;

import kafka.admin.AdminClient;
import kafka.common.OffsetAndMetadata;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.config.KafkaConfig;
import org.gnuhpc.bigdata.config.ZookeeperConfig;
import org.gnuhpc.bigdata.service.IKafkaAdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

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
}
