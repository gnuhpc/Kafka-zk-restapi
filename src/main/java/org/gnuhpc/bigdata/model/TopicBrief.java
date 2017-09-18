package org.gnuhpc.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicBrief {
    private String topic;
    private int numPartition;
    private long isrRate;
}
