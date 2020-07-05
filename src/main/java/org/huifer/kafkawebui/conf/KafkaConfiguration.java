package org.huifer.kafkawebui.conf;

import java.util.Properties;
import lombok.Data;
import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
@Data
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfiguration {
    private String brokerConnect;

    /**
     * 设置 kafka client 配置
     * @param properties
     */
    public void apply(Properties properties) {
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerConnect);
    }
}
