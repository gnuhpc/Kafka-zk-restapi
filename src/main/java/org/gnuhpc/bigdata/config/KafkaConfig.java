package org.gnuhpc.bigdata.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.service.KafkaConsumerService;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/** Created by gnuhpc on 2017/7/12. */
@Log4j2
@Data
@EnableKafka
//@Lazy
@Configuration
@Getter
public class KafkaConfig {

  @Value("${kafka.brokers}")
  private String brokers;

  @Value("${kafka.schemaregistry}")
  private String schemaregistry;

  @Value("${kafka.offset.topic}")
  private String internalTopic;

  @Value("${kafka.offset.partitions}")
  private int internalTopicPartitions;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @Value("${kafka.healthcheck.topic}")
  private String healthCheckTopic;

  @Value("${kafka.sasl.enable}")
  private boolean kafkaSaslEnabled;

  @Value("${kafka.sasl.security.protocol}")
  private String saslSecurityProtocol;

  @Value("${kafka.sasl.mechanism}")
  private String saslMechianism;

  @Lazy
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
    factory.getContainerProperties().setAckMode(ConcurrentMessageListenerContainer.AckMode.MANUAL);
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

  @Bean
  public Jackson2ObjectMapperBuilder objectMapperBuilder() {
    return new Jackson2ObjectMapperBuilder() {
      @Override
      public void configure(ObjectMapper objectMapper) {
        super.configure(objectMapper);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
      }
    };
  }
}
