package org.huifer.kafkawebui.model;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class ConsumerGroupOffsets {

    private String groupId;
    private Map<TopicPartition, OffsetAndMetadata> offsets;

    public boolean isEmp() {
        return offsets.isEmpty();
    }

    public ConsumerGroupOffsets forTopics(Set<String> topics) {
        final var filteredOffsets = offsets.entrySet().stream()
                .filter(e -> topics.contains(e.getKey().topic()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        return new ConsumerGroupOffsets(groupId, filteredOffsets);
    }
}
