package org.huifer.kafkawebui.service.kafka.impl;

import org.apache.kafka.clients.admin.NewTopic;
import org.huifer.kafkawebui.model.CreateTopicParam;
import org.huifer.kafkawebui.service.kafka.TopicOperation;

public class TopicOperationImpl implements TopicOperation {

    @Override
    public void createTopic(CreateTopicParam createTopicParam) {
        NewTopic newTopic = new NewTopic(createTopicParam.getName(),
                createTopicParam.getPartitionsNum(),
                createTopicParam.getReplicationFactor());


    }
}
