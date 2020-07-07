package org.huifer.kafkawebui.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SendMessageReq {
    private String topic;
    private String key;
    private String value;
}
