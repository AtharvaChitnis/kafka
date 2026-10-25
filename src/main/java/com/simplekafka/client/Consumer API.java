package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
public class SimpleKafkaConsumer {
    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    private long currentOffset;
    public SimpleKafkaConsumer( String bootstrapBroker, int bootstrapPort, String topic, int partition) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = 0;
    }
    public void initialize() throws IOException {
        client.initialize();
    }
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic,partition,currentOffset, 1024 * 1024);
        if(!messages.isEmpty()) {
            currentOffset += messages.size();
        }
        return messages;
    }
    //Additional methods
}

public List<byte[]> poll() throws IOException {
    List<byte[]> messages = client.fetch(topic, partition,currentOffset,MAX_BYTES);
    if(!messages.isEmpty()) {
        currentOffset += messages.size();
    }
    return messages;
}

public void startConsuming(MessageHandler handler) {
    if (running.compareAndSet(false, true)) {
        consumerThread = new Thread(() -> {
            try {
                while (running.get()) {
                    List<byte[]> messages = poll();

            for (byte[] message : messages) {
                                    handler.handle(message, currentOffset - messages.size() + messages.indexOf(message));
                                }
                                // If no messages, wait a bit before polling again
                                if (messages.isEmpty()) {
                                    Thread.sleep(POLL_INTERVAL_MS);
                                }
                            }
                        } catch (Exception e) {
                            if (running.get()) {
                                LOGGER.log(Level.SEVERE, "Error in consumer loop", e);
                            }
                            running.set(false);
                        }
                    });
                    consumerThread.setDaemon(true);
                    consumerThread.start();
                    LOGGER.info("Started consuming from topic: " + topic + ", partition: " + partition);
                }
            }

public void stopConsuming() {
    if (running.compareAndSet(true, false)) {
        if (consumerThread != null) {
            try {
                consumerThread.interrupt();
                consumerThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

LOGGER.info("Stopped consuming from topic: " + topic + ", partition: " + partition);
    }
}

public void seek(long offset) {
    this.currentOffset = offset;
}

public long getCurrentOffset() {
    return currentOffset;
}