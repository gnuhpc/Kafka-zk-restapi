package org.gnuhpc.bigdata.service;

import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.coordinator.group.OffsetKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.gnuhpc.bigdata.service.impl.KafkaAdminService;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

@Service
public class KafkaConsumerService {
    @Autowired
    private OffsetStorage offsetStorage;

    private final static int INTERVAL = 5 * 1000; // 5s interval

    /**
     * Listening offset thread method.
     */
    @KafkaListener(topics = "${kafka.offset.topic}")
    public void startOffsetListener(ConsumerRecord<ByteBuffer, ByteBuffer> record) {
        Map<GroupTopicPartition,OffsetAndMetadata> offsetMap;
        if (record.key() != null) {
            Object offsetKey = GroupMetadataManager.readMessageKey(record.key());
            if (offsetKey instanceof OffsetKey) {
                GroupTopicPartition groupTopicPartition = ((OffsetKey) offsetKey).key();
                if (offsetStorage.get(groupTopicPartition.group()) != null) {
                    offsetMap =offsetStorage.get(groupTopicPartition.group());
                } else {
                    offsetMap = new HashMap<>();
                }
                OffsetAndMetadata offsetValue = GroupMetadataManager.readOffsetMessageValue(record.value());
                offsetMap.put(groupTopicPartition,offsetValue);
                offsetStorage.put(groupTopicPartition.group(),offsetMap);
            }
        }

    }

}
