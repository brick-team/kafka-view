package org.huifer.kafkawebui.service.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.huifer.kafkawebui.serializer.Deserializers;
import org.huifer.kafkawebui.serializer.MessageDeserializer;

import java.util.List;

public interface IMessageOperation {
    boolean sendMessage(String topic, String key, String value);

    List<ConsumerRecord> getMessage(String topicName, Deserializers deserializer);
}
