package org.huifer.kafkawebui.service.kafka.impl;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.huifer.kafka.KafkaTopicDemo;
import org.huifer.kafkawebui.conf.KafkaConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * kafka 操作的超类, 主要定义 adminClient 以及 kafkaConfiguration
 * @see org.apache.kafka.clients.admin.AdminClient
 * @see org.apache.kafka.clients.consumer.KafkaConsumer
 */
public abstract class AbsKafkaOperation {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicDemo.class);

    AdminClient adminClient;
    KafkaConsumer<byte[], byte[]> kafkaConsumer;
    KafkaProducer<byte[], byte[]> kafkaProducer;


    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    @PostConstruct
    void initKafkaProducer(){
        LOG.info("开始初始化 kafka producer");
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.getBrokerConnect());
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        kafkaProducer = new KafkaProducer<byte[], byte[]>(properties);
    }

    @PostConstruct
    void initAdminClient() {
        LOG.info("开始初始化 admin Client");
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.getBrokerConnect());
        adminClient = AdminClient.create(properties);

    }

    @PostConstruct
    void initKafkaConsumer() {
        LOG.info("开始初始化 kafka consumer ");
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.getBrokerConnect());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        kafkaConsumer = new KafkaConsumer<>(properties);
    }

}
