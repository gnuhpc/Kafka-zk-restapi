package org.gnuhpc.bigdata.task;

import lombok.Data;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.Callable;

@Data
public class FetchOffsetFromZKTask implements Callable<FetchOffSetFromZKResult> {
    private String topic;
    private String consumerGroup;
    private int partition;

    private ZookeeperUtils zookeeperUtils;

    public FetchOffsetFromZKTask(ZookeeperUtils zookeeperUtils, String topic, String consumerGroup, int partition) {
        this.zookeeperUtils = zookeeperUtils;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.partition = partition;
    }

    @Override
    public FetchOffSetFromZKResult call() throws Exception {
        long offset = 0;
        FetchOffSetFromZKResult result = new FetchOffSetFromZKResult();
        String path = "/consumers/" + consumerGroup + "/offsets/" + topic + "/" + partition;
        if (zookeeperUtils.getCuratorClient().checkExists().forPath(path) != null) {
            offset = Long.parseLong(zookeeperUtils.getZkClient()
                    .readData("/consumers/" + consumerGroup +
                            "/offsets/" + topic + "/" + partition));
        }

        result.setOffset(offset);
        result.setTopic(topic);
        result.setParition(partition);
        return result;
    }
}
