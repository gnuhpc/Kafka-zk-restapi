package org.gnuhpc.bigdata.model;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;

import java.util.List;
import java.util.Properties;

/**
 * Created by gnuhpc on 2017/7/21.
 */
@Log4j
@Getter
@Setter
public class TopicMeta {
    private String topicName;
    private int partitionCount;
    private int replicationFactor;
    private List<TopicPartitionInfo> topicPartitionInfos;
    private Properties topicCustomConfigs;

    public TopicMeta(String topicName){
        this.topicName = topicName;
    }


}
