package org.huifer.kafkawebui.service.kafka.impl;

import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.huifer.kafkawebui.model.CreateTopicParam;
import org.huifer.kafkawebui.model.TopicPartitionVO;
import org.huifer.kafkawebui.model.TopicVO;
import org.huifer.kafkawebui.serializer.MessageDeserializer;
import org.huifer.kafkawebui.service.kafka.TopicOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class TopicOperationImpl extends AbsKafkaOperation implements TopicOperation {

    private static final Logger LOG = LoggerFactory.getLogger(TopicOperationImpl.class);

    @Override
    public boolean createTopic(CreateTopicParam createTopicParam) {

        NewTopic newTopic = new NewTopic(createTopicParam.getName(),
                createTopicParam.getPartitionsNum(),
                createTopicParam.getReplicationFactor());
        CreateTopicsResult topics = adminClient.createTopics(List.of(newTopic));
        boolean flg = false;

        try {
            topics.all().get();
            flg = true;
            LOG.info("Topic name=[{}] 创建成功.", newTopic.name());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Topic name=[{}] 创建失败, {}", newTopic.name(), e);
        }

        return flg;
    }

    @Override
    public boolean deleteTopic(String topic) {
        DeleteTopicsOptions deleteTopicsOptions = new DeleteTopicsOptions();
        deleteTopicsOptions.timeoutMs(5000);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(List.of(topic), deleteTopicsOptions);
        boolean flg = false;
        try {
            deleteTopicsResult.all().get();
            LOG.info("Topic name=[{}] 删除成功.", topic);
            flg = true;
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Topic name=[{}] 删除失败. {}", topic, e);

        }
        return flg;
    }

    @Override
    public Map<String, TopicVO> getTopics() {
        Set<String> topicSet = kafkaConsumer.listTopics().keySet();
        Map<String, TopicVO> result = new HashMap<>(topicSet.size());
        for (String topic : topicSet) {
            TopicVO topicInfo = topicInfo(topic);
            result.put(topic, topicInfo);
        }
        return result;
    }

    @Override
    public TopicVO topicInfo(String topic) {
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        TopicVO topicVO = new TopicVO();
        topicVO.setName(topic);

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

                topicPartitionVO.addReplica(new TopicPartitionVO.PartitionReplica(
                        replica.id(), isInSync, false, isInOffline)
                );
            }

            Node leader = partitionInfo.leader();
            if (leader != null) {
                topicPartitionVO.addReplica(new TopicPartitionVO.PartitionReplica(leader.id(), true, true, false));
            }
            partitions.put(partitionInfo.partition(), topicPartitionVO);
        }
        topicVO.setPartitionVOMap(partitions);
        return topicVO;
    }


}
