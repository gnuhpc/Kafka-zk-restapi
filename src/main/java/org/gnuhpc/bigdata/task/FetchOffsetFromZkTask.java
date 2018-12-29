package org.gnuhpc.bigdata.task;

import java.util.concurrent.Callable;
import lombok.Data;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;

@Data
public class FetchOffsetFromZkTask implements Callable<FetchOffSetFromZkResult> {

  private String topic;
  private String consumerGroup;
  private int partition;

  private ZookeeperUtils zookeeperUtils;

  public FetchOffsetFromZkTask(
      ZookeeperUtils zookeeperUtils, String topic, String consumerGroup, int partition) {
    this.zookeeperUtils = zookeeperUtils;
    this.topic = topic;
    this.consumerGroup = consumerGroup;
    this.partition = partition;
  }

  @Override
  public FetchOffSetFromZkResult call() throws Exception {
    long offset = 0;
    FetchOffSetFromZkResult result = new FetchOffSetFromZkResult();
    String path = "/consumers/" + consumerGroup + "/offsets/" + topic + "/" + partition;
    if (zookeeperUtils.getCuratorClient().checkExists().forPath(path) != null) {
      offset =
          Long.parseLong(
              zookeeperUtils
                  .getZkClient()
                  .readData("/consumers/" + consumerGroup + "/offsets/" + topic + "/" + partition));
    }

    result.setOffset(offset);
    result.setTopic(topic);
    result.setParition(partition);
    return result;
  }
}
