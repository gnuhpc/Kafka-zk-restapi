package org.gnuhpc.bigdata.model;

import lombok.*;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.constant.ConsumerState;
import org.gnuhpc.bigdata.constant.ConsumerType;

/**
 * Created by gnuhpc on 2017/7/27.
 */
@Setter
@Getter
@Log4j
@ToString
@EqualsAndHashCode
public class ConsumerGroupDesc implements Comparable<ConsumerGroupDesc> {
    private String groupName;
    private String topic;
    private int partitionId;
    private long currentOffset;
    private long logEndOffset;
    @Setter(AccessLevel.NONE)
    private long lag;
    private String consumerId;
    private String host="-";
    private ConsumerState state;
    private ConsumerType type;

    public void setLag() {
        this.lag = logEndOffset - currentOffset;
    }

    @Override
    public int compareTo(ConsumerGroupDesc o) {
        if (this.topic.equals(o.topic)) {
            if (this.partitionId == o.partitionId) {
                return 0;
            } else if (this.partitionId < o.partitionId) {
                return -1;
            } else {
                return 1;
            }
        } else {
            return this.topic.compareTo(this.topic);
        }
    }
}
