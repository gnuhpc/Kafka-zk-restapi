package org.gnuhpc.bigdata.config;

import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.service.KafkaConsumerService;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gnuhpc on 2017/7/12.
 */

@Log4j
@Data
@ConfigurationProperties(prefix = "kafka")
@EnableKafka
@Configuration
public class KafkaConfig {
    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.offset.topic}")
    private String internalTopic;

    @Value("${kafka.offset.partitions}")
    private int internalTopicPartitions;


    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public KafkaUtils kafkaUtils() {
        return new KafkaUtils();
    }

    @Bean
    public OffsetStorage offsetStorage() {
        return new OffsetStorage();
    }

    @Bean
    public KafkaConsumerService kafkaConsumerService() {
        return new KafkaConsumerService(internalTopicPartitions);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<ByteBuffer, ByteBuffer>>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<ByteBuffer, ByteBuffer> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL);
        return factory;
    }

    @Bean
    public DefaultKafkaConsumerFactory<ByteBuffer, ByteBuffer> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class);
        return props;
    }

}
