package org.gnuhpc.bigdata.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.constant.ConsumerState;

/**
 * Created by gnuhpc on 2017/7/27.
 */
@Setter
@Getter
@Log4j
@ToString
public class ConsumerGroupDesc {
    private String topic;
    private int partitionId;
    private long currentOffset;
    private long logEndOffset;
    private long lag;
    private String consumerId;
    private String host="-";
    private ConsumerState state;
}
