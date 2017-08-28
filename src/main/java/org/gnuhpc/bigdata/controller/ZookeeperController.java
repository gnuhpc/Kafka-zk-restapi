package org.gnuhpc.bigdata.controller;

import com.google.common.net.HostAndPort;
import io.swagger.annotations.Api;
import org.gnuhpc.bigdata.model.ZkServerEnvironment;
import org.gnuhpc.bigdata.model.ZkServerStat;
import org.gnuhpc.bigdata.service.ZookeeperService;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Created by gnuhpc on 2017/7/16.
 */
@RestController
@RequestMapping("/zk")
@Api(value = "Control Zookeeper with Rest API")
public class ZookeeperController {
    @Autowired
    private ZookeeperUtils zookeeperUtils;

    @Autowired
    private ZookeeperService zookeeperService;

    @GetMapping("/ls/{path}")
    public List<String> ls(@PathVariable("path") String path){
        try {
            return zookeeperUtils.getCuratorClient().getChildren().forPath("/"+path);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    @GetMapping("/connstate")
    public String zkConnState(){
        return zookeeperUtils.getCuratorClient().getState().toString();
    }

    @GetMapping("/ping")
    public String ping(){
        return "pong";
    }

    @GetMapping("/stat")
    public Map<HostAndPort,ZkServerStat> getStat(){
        return zookeeperService.stat();
    }

    @GetMapping("/env")
    public Map<HostAndPort,ZkServerEnvironment> getEnv(){
        return zookeeperService.environment();
    }


}
