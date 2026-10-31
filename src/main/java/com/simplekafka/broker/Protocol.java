package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public final class Protocol {
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    public static final byte REPLICATE = 0x05;
    public static final byte TOPIC_NOTIFICATION = 0x06;

    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte ERROR_RESPONSE = (byte) 0x99;

    private Protocol() {
    }

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        if (topic == null) {
            throw new IllegalArgumentException("topic cannot be null");
        }
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + topicBytes.length + 4 + message.length);
        buffer.put(PRODUCE);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        if (topic == null) {
            throw new IllegalArgumentException("topic cannot be null");
        }
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + topicBytes.length + 4 + 8 + 4);
        buffer.put(FETCH);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        if (topic == null) {
            throw new IllegalArgumentException("topic cannot be null");
        }
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + topicBytes.length + 4 + 2);
        buffer.put(CREATE_TOPIC);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        if (topic == null) {
            throw new IllegalArgumentException("topic cannot be null");
        }
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + topicBytes.length + 4 + 8 + 4 + message.length);
        buffer.put(REPLICATE);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeTopicNotification(String topic) {
        if (topic == null) {
            throw new IllegalArgumentException("topic cannot be null");
        }
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + topicBytes.length);
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);
        buffer.flip();
        return buffer;
    }

    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        if (channel == null || !channel.isOpen()) {
            return;
        }
        String payload = errorMessage == null ? "Unknown error" : errorMessage;
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + bytes.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}
