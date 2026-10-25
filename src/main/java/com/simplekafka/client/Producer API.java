package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
public class SimpleKafkaProducer {
    private final SimpleKafkaClient client;
    private final String topic;
    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
    }
    public void initialize() throws IOException {
        client.initialize();
    }
    public long send(String message) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        return client.send(topic,0, data); //Default to partition 0
    } 
    //Additional methods
}

public long send(String message) throws IOException

public long send(String message, int partition) throws IOException

public void close()
