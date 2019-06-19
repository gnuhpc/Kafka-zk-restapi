package org.gnuhpc.bigdata.controller;

import com.google.common.net.HostAndPort;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import java.util.Map;
import org.gnuhpc.bigdata.model.ZkServerEnvironment;
import org.gnuhpc.bigdata.model.ZkServerStat;
import org.gnuhpc.bigdata.service.ZookeeperService;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Created by gnuhpc on 2017/7/16. */
@RestController
@RequestMapping("/zk")
@Api(value = "Control Zookeeper with Rest API")
public class ZookeeperController {

  @Lazy
  @Autowired private ZookeeperUtils zookeeperUtils;

  @Autowired private ZookeeperService zookeeperService;

  @GetMapping("/ls/path")
  @ApiOperation(value = "List a zookeeper path")
  public List<String> ls(@RequestParam String path) {
    return zookeeperUtils.lsPath(path);
  }

  @GetMapping("/get/path")
  @ApiOperation(value = "Get data of a zookeeper path")
  public String get(@RequestParam String path) {
    return zookeeperUtils.getNodeData(path);
  }

  @GetMapping("/connstate")
  @ApiOperation(value = "Get the connection state of zookeeper")
  public String zkConnState() {
    return zookeeperUtils.getState();
  }

  @GetMapping("/stat")
  @ApiOperation(value = "Get the service state of zookeeper")
  public Map<HostAndPort, ZkServerStat> getStat() {
    return zookeeperService.stat();
  }

  @GetMapping("/env")
  @ApiOperation(value = "Get the environment information of zookeeper")
  public Map<HostAndPort, ZkServerEnvironment> getEnv() {
    return zookeeperService.environment();
  }
}
