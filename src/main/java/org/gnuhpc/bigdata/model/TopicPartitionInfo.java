package org.gnuhpc.bigdata.model;

import lombok.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.Node;

import java.util.List;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class TopicPartitionInfo implements Comparable<TopicPartitionInfo>
{
    private int partitionId;
    private String leader;
    private List<String> replicas;
    private List<String> isr;
    private boolean in_sync;
    private long startOffset;
    private long endOffset;
    @Setter(AccessLevel.NONE)
    private long messageAvailable;

    public void setIn_sync(){
        if(isr!=null && replicas !=null && isr.size() == replicas.size()){
            in_sync = CollectionUtils.isEqualCollection(isr,replicas);
        }
        else{
            in_sync = false;
        }
    }

    public void setMessageAvailable() {
        this.messageAvailable = this.endOffset - this.startOffset;
    }


    @Override
    public int compareTo(TopicPartitionInfo topicPartitionInfo) {
        if (this.partitionId < topicPartitionInfo.partitionId) return -1;
        else if (this.partitionId == topicPartitionInfo.partitionId) return 0;
        else return 1;
    }
}
