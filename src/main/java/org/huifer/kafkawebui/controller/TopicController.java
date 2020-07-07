package org.huifer.kafkawebui.controller;

import org.huifer.kafkawebui.model.CreateTopicParam;
import org.huifer.kafkawebui.model.ResultVO;
import org.huifer.kafkawebui.model.TopicVO;
import org.huifer.kafkawebui.service.kafka.TopicOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Set;

@RestController
@RequestMapping("/topic")
public class TopicController {
    @Autowired
    private TopicOperation topicOperation;

    @GetMapping("/list")
    public ResultVO list() {
        Set<String> topicNames = topicOperation.getTopics().keySet();
        return new ResultVO("ok", topicNames, 200);
    }

    @GetMapping("/info")
    public ResultVO info(
            @RequestParam String topic
    ) {
        TopicVO topicVO = topicOperation.topicInfo(topic);
        return new ResultVO("ok", topicVO, 200);
    }

    @GetMapping("/del")
    public ResultVO del(
            @RequestParam String topic
    ) {
        boolean b = topicOperation.deleteTopic(topic);
        return new ResultVO("ok", b, 200);
    }

    @PostMapping("/create")
    public ResultVO create(
            @RequestBody CreateTopicParam createTopicParam
    ) {
        boolean topic = topicOperation.createTopic(createTopicParam);
        return new ResultVO("ok", topic, 200);
    }
}
