package org.gnuhpc.bigdata.componet;

import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.GroupTopicPartition;
import lombok.Data;
import lombok.ToString;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Data
public class OffsetStorage {
    private static Map<String, Map<GroupTopicPartition,OffsetAndMetadata>> consumerOffsets = new ConcurrentHashMap<>();

    public void put(String consumerGroup,
                    Map<GroupTopicPartition,OffsetAndMetadata> offsetMap){
        consumerOffsets.put(consumerGroup, offsetMap);
    }

    public void clear(){
        consumerOffsets.clear();
    }

    public Map get(String consumerGroup){
        return consumerOffsets.get(consumerGroup);
    }

    @Override
    public String toString() {
        return consumerOffsets.toString();
    }
}
