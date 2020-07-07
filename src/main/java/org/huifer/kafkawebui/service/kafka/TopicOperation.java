package org.huifer.kafkawebui.service.kafka;

import org.huifer.kafkawebui.model.CreateTopicParam;
import org.huifer.kafkawebui.model.TopicVO;

import java.util.Map;

/**
 * kafka topic 操作
 * <ol>
 *     <li>创建 topic.</li>
 *     <li>删除 topic.</li>
 *     <li>获取一个 topic 信息.</li>
 *     <li>获取所有 topic 信息.</li>
 * </ol>
 */
public interface TopicOperation {

    /**
     * 创建topic
     * @param createTopicParam 创建topic的请求参数
     * @return true 成功, false 失败
     */
    boolean createTopic(CreateTopicParam createTopicParam);

    /**
     * 删除 topic
     * @param topic topic 名称
     * @return true 成功, false 失败
     */
    boolean deleteTopic(String topic);

    /**
     * 获取topic信息
     * @return key: topic name , value: topic 信息
     * @see org.huifer.kafkawebui.model.TopicVO
     */
    Map<String, TopicVO> getTopics();

    /**
     * 获取topic信息
     * @param topic topic名称
     * @return topic 信息
     * @see org.huifer.kafkawebui.model.TopicVO
     */
    TopicVO topicInfo(String topic);

}
