package org.huifer.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.huifer.kafkawebui.model.ClusterDescription;
import org.huifer.kafkawebui.model.TopicPartitionVO;
import org.huifer.kafkawebui.model.TopicPartitionVO.PartitionReplica;
import org.huifer.kafkawebui.model.TopicVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicDemo {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicDemo.class);

    /**
     * kafka admin client
     */
    private static final AdminClient adminClient;

    private static final KafkaConsumer<Byte[], Byte[]> kafkaConsumer;


    static {

        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:");

        adminClient = AdminClient.create(properties);

        properties.clear();

        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        kafkaConsumer = new KafkaConsumer<>(properties);

    }

    public static void main(String[] args) {
        createTopic();
        Map<String, TopicVO> topics = getTopics();
        deleteTopic("name");
        Map<String, TopicVO> topics2 = getTopics();

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

    /**
     * 创建 topic .
     */
    private static void createTopic() {
        NewTopic topic = new NewTopic("name", 0, (short) 0);
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
}
