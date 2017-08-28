package org.gnuhpc.bigdata.config;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * Created by gnuhpc on 2017/7/12.
 */

@Log4j
@Setter
@Getter
@ConfigurationProperties(prefix = "kafka")
@Component
@Configuration
public class KafkaConfig {
    private String brokers;

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public KafkaUtils kafkaUtils(){
        return new KafkaUtils();
    }

    @Bean
    public OffsetStorage offsetStorage(){
        return new OffsetStorage();
    }
}
