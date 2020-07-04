package org.huifer.kafkawebui.runner;

import org.huifer.kafkawebui.conf.KafkaConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

@Service
public class KafkaAdminClientRunner implements CommandLineRunner {
    private final KafkaConfiguration kafkaConfiguration;

    public KafkaAdminClientRunner(KafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;
    }

    @Override
    public void run(String... args) throws Exception {

        System.out.println();
    }
}
