package org.huifer.kafkawebui.conf;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
@Data
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfiguration {
    private String brokerConnect;
}
