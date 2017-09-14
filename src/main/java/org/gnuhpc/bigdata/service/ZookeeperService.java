package org.gnuhpc.bigdata.service;

import com.google.common.net.HostAndPort;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.constant.ZkServerCommand;
import org.gnuhpc.bigdata.model.ZkServerEnvironment;
import org.gnuhpc.bigdata.model.ZkServerStat;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.stream.Collectors;

@Service
@Log4j
public class ZookeeperService {
    @Autowired
    private ZookeeperUtils zookeeperUtils;

    public Map<HostAndPort, ZkServerStat> stat() {
        return zookeeperUtils.getZookeeperConfig().getHostAndPort().stream()
                .collect(Collectors.toMap(
                        hp -> hp,
                        hp -> zookeeperUtils.parseStatResult(
                                zookeeperUtils.executeCommand(
                                        hp.getHostText(),
                                        hp.getPort(),
                                        ZkServerCommand.stat.toString()
                                )
                        )
                ));
    }

    public Map<HostAndPort, ZkServerEnvironment> environment() {
        return zookeeperUtils.getZookeeperConfig().getHostAndPort().stream()
                .collect(Collectors.toMap(
                        hp -> hp,
                        hp -> zookeeperUtils.parseEnvResult(
                                zookeeperUtils.executeCommand(
                                        hp.getHostText(),
                                        hp.getPort(),
                                        ZkServerCommand.envi.toString()
                                )
                        )
                ));
    }


}
