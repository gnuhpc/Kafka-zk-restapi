package org.gnuhpc.bigdata.componet;

import kafka.common.OffsetAndMetadata;
import kafka.coordinator.GroupTopicPartition;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Data
public class OffsetStorage {
    private static Map<String, Map<GroupTopicPartition,OffsetAndMetadata>> consumerOffsets = new ConcurrentHashMap<>();

    public Map<String, Map<GroupTopicPartition, OffsetAndMetadata>> getMap() {
        return consumerOffsets;
    }

    public void put(String consumerGroup,
                    Map<GroupTopicPartition,OffsetAndMetadata> offsetMap){
        if (offsetMap != null) {
            consumerOffsets.put(consumerGroup, offsetMap);
        }
    }

    public void clear(){
        consumerOffsets.clear();
    }

    public Map get(String consumerGroup){
        return consumerOffsets.get(consumerGroup);
    }

    public void remove(String consumerGroup){
        consumerOffsets.remove(consumerGroup);
    }

    @Override
    public String toString() {
        return consumerOffsets.toString();
    }
}
