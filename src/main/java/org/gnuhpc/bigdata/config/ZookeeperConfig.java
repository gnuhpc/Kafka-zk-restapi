package org.gnuhpc.bigdata.config;

import com.google.common.net.HostAndPort;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/** Created by gnuhpc on 2017/7/16. */
@Log4j
@Setter
@Getter
@ConfigurationProperties(prefix = "zookeeper")
@Lazy
@Component
@Configuration
public class ZookeeperConfig {

  private String uris;

  @Bean(initMethod = "init", destroyMethod = "destroy")
  public ZookeeperUtils zookeeperUtils() {
    return new ZookeeperUtils();
  }

  public List<HostAndPort> getHostAndPort() {
    return Arrays.stream(uris.split(",")).map(HostAndPort::fromString).collect(Collectors.toList());
  }
}
