public simpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
    this.brokerId = brokerId;
    this.brokerHost = host;
    this.brokerPort = port;
    this.topics = new ConcurrentHashMap<>();
    this.executor =Executors.newFixedThreadPool(10);
    this.serverChannel = ServerSocketChannel.open();
    this.isRunning = new AtomicBoolean(false);
    this.isController = new AtomixBoolean(false);
    this.clusterMetadata = new ConcurretnHashMap<>();

    //Intialize data directory
    File dataDir = new File(DATA_DIR + File.separator + brokerId);
    if(!dataDir.exists()) {
        dataDir.mkdirs();
    }
    //Intialize ZooKeeper client
    this.zkClient= new ZookeeperCLient("localhost", zkPort);
}

public void start() throws IOException {....}

public void stop() {...}

private void registerWithZooKeeper() { ... }

private void onBrokersChanged(List<String> brokerIds) {
    /**
     * Update cluster metadata
     * Remove brokers that have disappeared 
     * Re-elect controller if needed
     * As controller, rebalance partitions due to cluster changes
     * Re-attempt controller election
     */
}

private void electController() {
    try {
        String controllerPath = "/controller"

        //Check if node exists and is valid
        boolean nodeExists = zkClient.exists(controllerPath);
        if(nodeExists) {
            String existingData = zkClient.getData(controllerPath);
            if(existingData == null || existingData.trim().isEmpty()) {
                zkClient.deleteNode(controllerPath);
                nodeExists = false;
            }
        }
        // Try to become controller
        boolean becameController = false;
        if(!nodeExists) {
            becameController = zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId));
        }
        if(becameController) {
            isController.set(true)
            rebalancePartitions();
        } else {
            //Watch for controller changes
            zkClient.watchNode(controllerPath, this::onControllerChange);
        }
        catch (Exception e) {
            LOGGER.log(Level.SEVERE, "COntroller election failed", e);
            //REtry election after delay
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }
}

private void createTopic (String topic, int numPartitions, short replicationFactor) {
    /**
     * Create topic directory
     * Create topic in ZooKeeper
     * Create partitions
     * Select leader and followers
     * Create partitions
     * Store partitions metadata in ZooKeeper
     * Add topic to broker's metadata
     * Notify all brokers to load the topic
     */
}

private void loadTopic(String topic) throws Exception { ... }

private void rebalancePartitions() { ... }

private void handleProduceRequest(SocketChannel, clientChannel, ByteBuffer buffer) throws IOException {
    /**
     * Parse request data
     * Check if topic exists
     * Find the partition
     * Check if this broker is the leader for the partition
     * Forward to leader
     * Append message to log
     * REplicate to followers
     * Send acknowledgement to client
     */
}

private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) thorws IOException {
    /**
     * Parse request data
     * Check if topic exists
     * Find the partition
     * Check if the offset is valid
     * No messages available at this offset
     * Read messages from log
     * Send response
     * 1 byte for response type, 4 bytes for message count
     * 8 bytes for offset, 4 bytes for length, plus message bytes
     */
}

private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
    //Prepare replication request
    //Send request to follower
    //Read acknowledgement
}

private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
    /**Parse request data
     * verify topic and partition
     * Find the partition
     * Append message to log (As follower)
     * Send acknowledgment
     */
}

private void acceptConnections() {
    while(isRunning.get()) {
        try {
            SocketChannel clientChannel = serverChannel.accept();
            if(clientChannel != null) {
                clientChannel.configureBlocking(false);

                //Handle client connection in a separate thread
                executor.submit(() -> handleClient(clientChannel));
            }
            Thread.sleep(100); //Small pause to prevent CPU spin
        } catch (Exception e) {
            if (isRunning.get()) {
                LOGGER.log(LEvel.SEVERE, "Error accepting connection", e);
            }
        }
    }
}
private void handleClient(SocketChannel clientChannel) {
    try {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        while (clientChannel.isOpen() && isRunning.get()) {
            buffer.clear();
            int bytesRead = clientChannel.read(buffer);
            if (bytesRead > 0) {
                buffer.flip();
                //Process the message based on protocol
                processClientMessage(clientChannel, buffer);
            } else if (byteRead < 0) {
                //Connection closed by client
                clientChannel.close();
                break;
            }
            Thread.sleep(50); //Small pause to prevent CPU spin
        }
    } catch (Exception e {
        //Handle errors
    } finally {
        try {
            if (clientChannel.isOpen()) {
                clientChannel.close();
            }
            catch (IOException e) {
                LOGGER.log(Level.Warning, "Error closing client channel", e);
            }
        }
    }
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();
        switch(messageType) {
            case Protocol.PRODUCE:
                handleProduceREquest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protoco.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType) ;
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }
    )
}