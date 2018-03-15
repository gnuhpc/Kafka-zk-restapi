package org.gnuhpc.bigdata.controller;

import io.swagger.annotations.*;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.model.JMXMetricData;
import org.gnuhpc.bigdata.service.CollectorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.util.List;

@Log4j
@RestController
@Validated
@Api(value = "/jmx", description = "Rest API for Collecting JMX Metric Data")
public class CollectorController {
  private static final String IP_AND_PORT_LIST_REGEX = "(([0-9]+(?:\\.[0-9]+){3}:[0-9]+,)*([0-9]+(?:\\.[0-9]+){3}:[0-9]+)+)|(default)";
  @Autowired
  private CollectorService collectorService;
  @Value("${jmx.kafka.jmxurl}")
  private String jmxKafkaURL;

  @GetMapping("/jmx")
  @ApiOperation(value = "Fetch JMX Metric Data")
  public List<JMXMetricData> collectJMXMetric(
          @Pattern(regexp = IP_AND_PORT_LIST_REGEX)@RequestParam @ApiParam(
                  value = "Parameter jmxurl should be a comma-separated list of {IP:Port} or set to \'default\'")String jmxurl) {
    if(jmxurl.equals("default")) {
      jmxurl = jmxKafkaURL;
    }

    System.out.println("jmxurl=" + jmxurl);
    /**
     * 1. jmxurl传值为空，使用项目中配置的jmxurl -- ok
     * 2. rmi超时连接时间设置问题，服务同步or异步
     * 3. 异常处理--用RestErrorResponse, CustomErrorController里的error返回结果修改
     * 4. swagger用法
     * 5. 定时？
     */
    log.debug("Collect JMX Metric Data Started.");
    return collectorService.collectJMXData(jmxurl);
  }

  @PostMapping(value = "/jmx2/{var}/tmp")
  public List<JMXMetricData>  writeMessage(@Size(min=1, max=3)@PathVariable String var,
                                           @Size(min=2, max=4)@RequestBody String testex) {
    return collectorService.collectJMXData("localhost:29999");
  }
}
