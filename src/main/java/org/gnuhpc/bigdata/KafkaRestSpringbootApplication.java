package org.gnuhpc.bigdata;

import lombok.extern.log4j.Log4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Log4j
public class KafkaRestSpringbootApplication {
	public static void main(String[] args) {
	    log.info("+++++++++Kafka-zk Rest Application starting++++++++++");
		SpringApplication.run(KafkaRestSpringbootApplication.class, args);
	}
}
