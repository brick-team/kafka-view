package org.huifer.kafkawebui.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.huifer.kafkawebui.model.ResultVO;
import org.huifer.kafkawebui.model.SendMessageReq;
import org.huifer.kafkawebui.serializer.Deserializers;
import org.huifer.kafkawebui.serializer.msg.StringMessageDeserializer;
import org.huifer.kafkawebui.service.kafka.IMessageOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RequestMapping("/message")
@RestController
public class MessageController {
    @Autowired
    private IMessageOperation messageOperation;

    @GetMapping("/info")
    public ResultVO info(
            @RequestParam String topic
    ) {
        Deserializers deserializers = new Deserializers(new StringMessageDeserializer(), new StringMessageDeserializer());
        List<ConsumerRecord> message = messageOperation.getMessage(topic, deserializers);
        return new ResultVO("ok", message, 200);
    }

    @PostMapping("/send")
    public ResultVO send(
            @RequestBody SendMessageReq sendMessageReq
    ) {
        boolean b = messageOperation.sendMessage(sendMessageReq.getTopic(), sendMessageReq.getKey(), sendMessageReq.getValue());
        return new ResultVO("ok", b, 200);
    }
}
