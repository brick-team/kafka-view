package org.huifer.kafkawebui.runner;

import java.util.Properties;
import javax.annotation.PostConstruct;
import org.huifer.kafkawebui.conf.KafkaConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Service;

@Service
public class KafkaAdminClientRunner implements Ordered {

    private final KafkaConfiguration kafkaConfiguration;

    public KafkaAdminClientRunner(KafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;
    }

    @PostConstruct
    public void init() {
        Properties properties = new Properties();
        kafkaConfiguration.apply(properties);
        System.out.println();

    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

}
