package org.huifer.kafkawebui.service.kafka;

import org.huifer.kafkawebui.model.CreateTopicParam;

public interface TopicOperation {

    void createTopic(CreateTopicParam createTopicParam);


}
