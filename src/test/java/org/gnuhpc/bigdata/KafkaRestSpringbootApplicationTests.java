package org.gnuhpc.bigdata;

import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.network.BlockingChannel;
import kafka.utils.ZkUtils;
import lombok.extern.log4j.Log4j;
import org.apache.curator.framework.CuratorFramework;
import org.gnuhpc.bigdata.model.BrokerInfo;
import org.gnuhpc.bigdata.service.impl.KafkaAdminService;
import org.gnuhpc.bigdata.utils.KafkaUtils;
import org.gnuhpc.bigdata.utils.ZookeeperUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@Log4j
public class KafkaRestSpringbootApplicationTests {

	@Autowired
	private KafkaAdminService kafkaAdminService;

	@Autowired
	private ZookeeperUtils zookeeperUtils;

	@Autowired
    private KafkaUtils kafkaUtils;

	private ZkUtils zkUtils;

	private CuratorFramework zkCuratorClient;
	@Before
	public void before(){
		this.zkUtils = zookeeperUtils.getZkUtils();
		this.zkCuratorClient = zookeeperUtils.getCuratorClient();
	}

	@Test
	public void contextLoads() {
		return;
	}

	@Test
	public void testListBrokers() throws Exception {
		List<BrokerInfo> brokerInfoList = kafkaAdminService.listBrokers();
		brokerInfoList.stream().forEach(log::info);
	}

	@Test
	public void testKafkaUtils(){
        BlockingChannel channel = new BlockingChannel("hadoop1", 9092,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                5000 /* read timeout in millis */);
        channel.connect();

        final String MY_GROUP = "test-consumer-group";
        final String MY_CLIENTID = "demoClientId";
        int correlationId = 0;
        final TopicAndPartition testPartition0 = new TopicAndPartition("testtopic2", 0);
        final TopicAndPartition testPartition1 = new TopicAndPartition("testtopic2", 1);
        final TopicAndPartition testPartition2 = new TopicAndPartition("testtopic2", 2);
        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        partitions.add(testPartition0);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                MY_GROUP,
                partitions,
                (short) 1 /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                correlationId,
                MY_CLIENTID);
        channel.send(fetchRequest.underlying());
        OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload());
        OffsetMetadataAndError result = fetchResponse.offsets().get(testPartition0);
        short offsetFetchErrorCode = result.error();
        if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
            channel.disconnect();
            // Go to step 1 and retry the offset fetch
        } else {
            long retrievedOffset = result.offset();
            String retrievedMetadata = result.metadata();
        }

        channel.disconnect();
    }
}
