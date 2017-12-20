package org.gnuhpc.bigdata.model;

import lombok.*;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.TopicPartition;
import org.gnuhpc.bigdata.constant.ConsumerState;
import org.gnuhpc.bigdata.constant.ConsumerType;
import org.gnuhpc.bigdata.utils.KafkaUtils;

import java.util.Map;

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

    private ConsumerGroupDesc(Builder builder) {
        setGroupName(builder.groupName);
        setTopic(builder.topic);
        setPartitionId(builder.partitionId);
        setCurrentOffset(builder.currentOffset);
        setLogEndOffset(builder.logEndOffset);
        lag = builder.logEndOffset-builder.currentOffset;
        setConsumerId(builder.consumerId);
        setHost(builder.host);
        setState(builder.state);
        setType(builder.type);
    }

    public static Builder newBuilder() {
        return new Builder();
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

    public static final class Builder {
        private String groupName;
        private String topic;
        private int partitionId;
        private long currentOffset;
        private long logEndOffset;
        private long lag;
        private String consumerId;
        private String host;
        private ConsumerState state;
        private ConsumerType type;

        private Builder() {
        }

        public Builder setGroupName(String val) {
            groupName = val;
            return this;
        }

        public Builder setTopic(String val) {
            topic = val;
            return this;
        }

        public Builder setPartitionId(int val) {
            partitionId = val;
            return this;
        }

        public Builder setCurrentOffset(long val) {
            currentOffset = val;
            return this;
        }

        public Builder setLogEndOffset(long val) {
            logEndOffset = val;
            return this;
        }

        public Builder setLag(long val) {
            lag = val;
            return this;
        }

        public Builder setConsumerId(String val) {
            consumerId = val;
            return this;
        }

        public Builder setHost(String val) {
            host = val;
            return this;
        }

        public Builder setState(ConsumerState val) {
            state = val;
            return this;
        }

        public Builder setType(ConsumerType val) {
            type = val;
            return this;
        }

        public ConsumerGroupDesc build() {
            return new ConsumerGroupDesc(this);
        }
    }
}
