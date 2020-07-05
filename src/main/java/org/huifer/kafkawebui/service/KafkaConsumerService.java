package org.huifer.kafkawebui.service;

import java.util.Properties;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.huifer.kafkawebui.conf.KafkaConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

public final class KafkaConsumerService {


    private KafkaConsumer<Byte[], byte[]> kafkaConsumer;
    @Autowired
    private KafkaConfiguration kafkaConfiguration;


    @PostConstruct
    public void initKafkaConsumerClient() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfiguration.getBrokerConnect());
        kafkaConsumer = new KafkaConsumer<>(properties);
    }


}
