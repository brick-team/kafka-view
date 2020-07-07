package org.huifer.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class KafkaSendDemo {


    /**
     * kafka admin client
     */
    private static final AdminClient adminClient;

    private static final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private static final KafkaProducer<byte[], byte[]> producer;

    static {

        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        adminClient = AdminClient.create(properties);

        properties.clear();

        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        kafkaConsumer = new KafkaConsumer<>(properties);

        properties.clear();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");  //设置kafka的连接和端口
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerDemo");  //
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        producer = new KafkaProducer<byte[], byte[]>(properties);
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> aaa =
                producer.send(new ProducerRecord<>("aaa", "message".getBytes(), "aaa".getBytes()));
        RecordMetadata recordMetadata = aaa.get();

        final int giveUp = 100;
        int noRecordsCount = 0;


        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor("aaa");
        List<TopicPartition> partitions = partitionInfos.stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(),
                        partitionInfo.partition())).collect(Collectors.toList());

        Map<TopicPartition, Long> topicPartitionLongMap = kafkaConsumer.endOffsets(partitions);

        for (TopicPartition partition : partitions) {
            kafkaConsumer.seek(partition, topicPartitionLongMap.get(partition));
        }


        System.out.println("============");
    }

    private static void msg() {
        kafkaConsumer.subscribe(List.of("aaa"));

        try {
            while (true) {
                ConsumerRecords<byte[], byte[]> poll = kafkaConsumer.poll(100);
                for (ConsumerRecord<byte[], byte[]> record : poll) {
                    byte[] value = record.value();
                    String valueStr = new String(value);
                    byte[] key = record.key();
                    String keyStr = new String(key);
                    System.out.println("key = " + keyStr + " value = " + valueStr);
                }
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}
