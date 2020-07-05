package org.huifer.kafkawebui.serializer;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface MessageDeserializer {

    String deserializeMessage(ByteBuffer buffer);
}
