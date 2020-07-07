package org.huifer.kafkawebui.service.kafka.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.huifer.kafkawebui.serializer.Deserializers;
import org.huifer.kafkawebui.serializer.MessageDeserializer;
import org.huifer.kafkawebui.service.kafka.IMessageOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Service
public class IMessageOperationImpl extends AbsKafkaOperation implements IMessageOperation {
    private static final Logger LOG = LoggerFactory.getLogger(IMessageOperationImpl.class);

    private String deserialize(MessageDeserializer deserializer, byte[] value) {
        return value != null ? deserializer.deserializeMessage(ByteBuffer.wrap(value)) : "";
    }

    @Override
    public boolean sendMessage(String topic, String key, String value) {
        Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(topic, key.getBytes(), value.getBytes()));
        try {
            send.get();
            LOG.info("发送消息成功 topic = [{}] key = [{}] value = [{}]", topic, key, value);
            return true;
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("消息发送失败,{}", e);
        }
        return false;
    }

    @Override
    public List<ConsumerRecord> getMessage(String topicName, Deserializers deserializer) {
        // default all
        int count = Integer.MIN_VALUE;

        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topicName);
        List<TopicPartition> partitions = partitionInfos.stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(),
                        partitionInfo.partition())).collect(Collectors.toList());

        // 指定消费的分区
        kafkaConsumer.assign(partitions);

        // key:topic value: size
        Map<TopicPartition, Long> topicPartitionLongMap = kafkaConsumer.endOffsets(partitions);

        for (TopicPartition partition : partitions) {
            long offset = Math.max(0, topicPartitionLongMap.get(partition) - 1);
            kafkaConsumer.seek(partition, Math.max(0, offset - count));
        }

        int totalCount = count * partitions.size();
        Map<TopicPartition, ArrayList<ConsumerRecord<byte[], byte[]>>> rawRecords = partitions
                .stream()
                .collect(Collectors.toMap(p -> p, p -> {
                    return new ArrayList(count);
                }));

        boolean moreRecords = true;
        // loop for records data
        while (rawRecords.size() < totalCount && moreRecords) {
            ConsumerRecords<byte[], byte[]> poll = kafkaConsumer.poll(Duration.ofMillis(300));
            moreRecords = false;
            for (TopicPartition partition : poll.partitions()) {
                List<ConsumerRecord<byte[], byte[]>> records = poll.records(partition);
                if (!records.isEmpty()) {
                    rawRecords.get(partition).addAll(records);
                    moreRecords = records.get(records.size() - 1).offset()
                            < topicPartitionLongMap.get(partition) - 1;
                }
            }
        }
        return rawRecords.values().stream().flatMap(Collection::stream)
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
                            deserialize(deserializer.getKeyDeserializer(), r.key()),
                            deserialize(deserializer.getValueDeserializer(), r.value()),
                            r.headers(),
                            r.leaderEpoch()
                    );
                }).collect(Collectors.toList());

    }
}
