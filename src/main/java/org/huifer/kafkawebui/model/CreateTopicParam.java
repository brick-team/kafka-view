package org.huifer.kafkawebui.model;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class CreateTopicParam {

    /**
     * topic 名称
     */
    private String name;
    /**
     * partitions 编号, 分区数量
     * <b>default value = 0 </b>
     */
    private Integer partitionsNum = 0;
    /**
     * 副本数量
     * <b>default value = 0 </b>
     */
    private Short replicationFactor = 0;

}
