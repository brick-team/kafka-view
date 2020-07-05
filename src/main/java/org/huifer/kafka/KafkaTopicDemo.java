package org.huifer.kafka;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.huifer.kafkawebui.model.BrokerVO;
import org.huifer.kafkawebui.model.ClusterDescription;
import org.huifer.kafkawebui.model.ConsumerGroupOffsets;
import org.huifer.kafkawebui.model.TopicPartitionVO;
import org.huifer.kafkawebui.model.TopicPartitionVO.PartitionReplica;
import org.huifer.kafkawebui.model.TopicVO;
import org.huifer.kafkawebui.serializer.Deserializers;
import org.huifer.kafkawebui.serializer.MessageDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicDemo {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicDemo.class);

    /**
     * kafka admin client
     */
    private static final AdminClient adminClient;

    private static final KafkaConsumer<byte[], byte[]> kafkaConsumer;


    static {

        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        adminClient = AdminClient.create(properties);

        properties.clear();

        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        kafkaConsumer = new KafkaConsumer<>(properties);

    }

    public static void main(String[] args) {
//        createTopic();
        Map<String, TopicVO> topics = getTopics();
//        deleteTopic("name");
//        Map<String, TopicVO> topics2 = getTopics();
        ClusterDescription clusterDescription = describeCluster();
        List<BrokerVO> brokers = getBrokers();
        getConsumerOffsets(List.of("name"));
        System.out.println();
    }

    /**
     * kafka 集群管理
     */
    private static ClusterDescription describeCluster() {
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        try {
            Collection<Node> nodes = describeClusterResult.nodes().get();
            KafkaFuture<Node> controller = describeClusterResult.controller();
            String clusterId = describeClusterResult.clusterId().get();

            ClusterDescription clusterDescription = new ClusterDescription();
            clusterDescription.setClusterId(clusterId);
            clusterDescription.setController(controller.get());
            clusterDescription.setNodes(nodes);
            return clusterDescription;
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("获取卡夫卡集群失败");

        }
        return null;
    }

    private static void getConsumerOffsets(List<String> topicNames) {
        Set<String> consumers = getConsumers();
        List<ConsumerGroupOffsets> list = new ArrayList<>();

        for (String consumer : consumers) {
            ConsumerGroupOffsets consumerGroupOffsets = resolveOffsets(consumer);
            ConsumerGroupOffsets consumerGroupOffsets1 = consumerGroupOffsets
                    .forTopics(topicNames.stream().collect(Collectors.toSet()));
            if (consumerGroupOffsets1.isEmp()) {
                list.add(consumerGroupOffsets1);
            }
        }
        System.out.println();
    }

    private static Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String groupID) {
        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient
                .listConsumerGroupOffsets(groupID);
        try {
            return listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return Collections.emptyMap();

    }


    private static void getMessage(String topicName, int count, Deserializers deserializers) {
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topicName);
        List<TopicPartition> partitions = partitionInfos.stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(),
                        partitionInfo.partition())).collect(Collectors.toList());

        kafkaConsumer.assign(partitions);

        Map<TopicPartition, Long> topicPartitionLongMap = kafkaConsumer.endOffsets(partitions);

        for (TopicPartition partition : partitions) {
            long offset = Math.max(0, topicPartitionLongMap.get(partition) - 1);
            kafkaConsumer.seek(partition, Math.max(0, offset - count));
        }

        int totalCount = count * partitions.size();
        Map<TopicPartition, ArrayList<ConsumerRecord<byte[], byte[]>>> rawRecords = partitions
                .stream()
                .collect(Collectors.toMap(p -> p, p -> new ArrayList(count)));

        boolean moreRecords = true;
        while (rawRecords.size() < totalCount && moreRecords) {
            ConsumerRecords<byte[], byte[]> poll = kafkaConsumer.poll(Duration.ofMillis(300));
            moreRecords = false;
            for (TopicPartition partition : poll.partitions()) {
                List<ConsumerRecord<byte[], byte[]>> records = poll.records(partition);
                if (!records.isEmpty()) {
                    rawRecords.get(partition).addAll(records);
                    Object object;
                    moreRecords = records.get(records.size() - 1).offset()
                            < topicPartitionLongMap.get(partition) - 1;
                }
            }
        }
        List<ConsumerRecord> collect = rawRecords.values().stream().flatMap(Collection::stream)
                .map(r -> {
                    return new ConsumerRecord(
                            r.topic(),
                            r.partition(),
                            r.offset(),
                            r.timestamp(),
                            r.timestampType(),
                            0l,
                            r.serializedKeySize(),
                            r.serializedValueSize(),
                            deserialize(deserializers.getKeyDeserializer(), r.key()),
                            deserialize(deserializers.getValueDeserializer(), r.value()),
                            r.headers(),
                            r.leaderEpoch()
                    );
                }).collect(Collectors.toList());

    }

    private static String deserialize(MessageDeserializer keydeserializer, byte[] value) {
        return value != null ? keydeserializer.deserializeMessage(ByteBuffer.wrap(value)) : "";
    }

    private static Set<String> getConsumers() {

        try {
            Collection<ConsumerGroupListing> consumerGroupListings = adminClient
                    .listConsumerGroups()
                    .all().get();
            return consumerGroupListings.stream().map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toSet());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();

        }
        return new HashSet<>();
    }

    private static List<BrokerVO> getBrokers() {
        List<BrokerVO> result = new ArrayList<>();
        ClusterDescription clusterDescription = describeCluster();
        if (clusterDescription != null) {
            for (Node node : clusterDescription.getNodes()) {
                boolean controller = node.id() == clusterDescription.getController().id();
                BrokerVO brokerVO = new BrokerVO();
                brokerVO.setId(node.id());
                result.add(brokerVO);
            }
        }
        return result;
    }

    /**
     * 创建 topic .
     */
    private static void createTopic() {
        NewTopic topic = new NewTopic("name", 1, (short) 1);
        CreateTopicsResult topics = adminClient.createTopics(List.of(topic));
        try {
            topics.all().get();
            LOG.info("Topics {} 创建成功", topic.name());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Topics {} 创建失败", topic.name());
            e.printStackTrace();
        }
    }

    /**
     * 删除 topic.
     *
     * @param topicName
     */
    private static void deleteTopic(String topicName) {
        DeleteTopicsOptions deleteTopicsOptions = new DeleteTopicsOptions();
        deleteTopicsOptions.timeoutMs(5000);
        DeleteTopicsResult deleteTopicsResult = adminClient
                .deleteTopics(List.of(topicName), deleteTopicsOptions);
        try {
            deleteTopicsResult.all().get();
            LOG.info("topics {} 删除成功", topicName);
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("topic {} 删除失败", topicName);
            e.printStackTrace();
        }
    }

    /**
     * 获取所有的 topic 信息
     *
     * @return
     */
    private static Map<String, TopicVO> getTopics() {
        Set<String> topicSet = kafkaConsumer.listTopics().keySet();

        Map<String, TopicVO> result = new HashMap<>(topicSet.size());
        for (String topic : topicSet) {
            TopicVO topicInfo = getTopicInfo(topic);
            result.put(topic, topicInfo);
        }
        return result;
    }

    /**
     * 单个topic的信息
     *
     * @param topicName
     * @return
     */
    private static TopicVO getTopicInfo(String topicName) {
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topicName);
        TopicVO topicVO = new TopicVO();
        topicVO.setName(topicName);

        Map<Integer, TopicPartitionVO> partitions = new TreeMap<>();

        for (PartitionInfo partitionInfo : partitionInfos) {
            int partition = partitionInfo.partition();
            TopicPartitionVO topicPartitionVO = new TopicPartitionVO();
            topicPartitionVO.setId(partition);

            Set<Integer> inSyncReplicas = Arrays.stream(partitionInfo.inSyncReplicas())
                    .map(Node::id)
                    .collect(Collectors.toSet());
            Set<Integer> offlineReplicas = Arrays.stream(partitionInfo.offlineReplicas())
                    .map(Node::id)
                    .collect(Collectors.toSet());

            for (Node replica : partitionInfo.replicas()) {
                boolean isInSync = inSyncReplicas.contains(replica.id());
                boolean isInOffline = offlineReplicas.contains(replica.id());

                topicPartitionVO.addReplica(new PartitionReplica(
                        replica.id(), isInSync, false, isInOffline)
                );
            }

            Node leader = partitionInfo.leader();
            if (leader != null) {
                topicPartitionVO.addReplica(new PartitionReplica(leader.id(), true, true, false));
            }
            partitions.put(partitionInfo.partition(), topicPartitionVO);
        }
        topicVO.setPartitionVOMap(partitions);
        return topicVO;
    }

    private static ConsumerGroupOffsets resolveOffsets(String groupId) {
        return new ConsumerGroupOffsets(groupId, listConsumerGroupOffsets(groupId));
    }
}
