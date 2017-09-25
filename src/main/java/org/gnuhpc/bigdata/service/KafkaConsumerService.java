package org.gnuhpc.bigdata.service;

import kafka.common.OffsetAndMetadata;
import kafka.coordinator.GroupMetadataManager;
import kafka.coordinator.GroupTopicPartition;
import kafka.coordinator.OffsetKey;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.gnuhpc.bigdata.componet.OffsetStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Log4j
public class KafkaConsumerService implements ConsumerSeekAware {
    @Autowired
    private OffsetStorage offsetStorage;

    private boolean[] resetInitArray;

    private final ThreadLocal<ConsumerSeekCallback> seekCallBack = new ThreadLocal<>();

    public KafkaConsumerService(int internalTopicPartitions) {
        this.resetInitArray = new boolean[internalTopicPartitions];
        Arrays.fill(resetInitArray, true);
    }

    /**
     * Listening offset thread method.
     */
    @KafkaListener(topics = "${kafka.offset.topic}")
    public void onMessage(
            ConsumerRecord<ByteBuffer, ByteBuffer> record,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        //set the offset of the partition being processed to the beginning. If already set, ignore it.
//        if (resetInitArray[partition]) {
//            long beginningOffset = kafkaAdminService.getBeginningOffset(topic, partition);
//            this.seekCallBack.get().seek(topic, partition, beginningOffset + 1);
//            resetInitArray[partition] = false;
//        }

        //Parse the commit offset message and store it in offsetStorage
        Map<GroupTopicPartition,OffsetAndMetadata> offsetMap;
        if (record.key() != null) {
            Object offsetKey = GroupMetadataManager.readMessageKey(record.key());
            log.debug(offsetKey);
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

    @Override
    public void registerSeekCallback(ConsumerSeekCallback consumerSeekCallback) {
        //set the seekcallback for resetting the offset
        this.seekCallBack.set(consumerSeekCallback);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> map, ConsumerSeekCallback consumerSeekCallback) {
        System.out.println();
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> map, ConsumerSeekCallback consumerSeekCallback) {
    }
}
