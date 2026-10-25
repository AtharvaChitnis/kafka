public void connect() throws IOException, InterruptedException {
    zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
    conncetedSignal.await();

    //Create required paths if they don't exist
    createPath("/brokers");
    createPath("/topics");
    createPath("/controller");
}

public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
    Stat stat = zooKeeper.exists(path, false);
    if(stat == null) {
        zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info("Created persistent node: " + path);
    } else {
        zooKeeper.setData(path,data.getBytes(), -1);
        LOGGER.info("Updated persistent node: " + path);
    }
}

public boolean createEphemeralNode(String path, String data) throws KeeperException,InterruptedException {
    Stat stat = zooKeeper.exists(path, false);
    if (stat= null) {
        zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        LOGGER.info("Created ephemeral node: " + path);
        return true;
    } else {
        LOGGER.info("Ephemeral node already exists: " + path);
        return false;
    }
}

public void watchChildren(String path, ChildrenCallback callback) {
    try {
        List<String> children = zooKeeper.getChildren(path, event -> {
            if (event.getType() == Wathcer.Event.EventType.NodeChildrenChanged) {
                try {
                    List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                        if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                            watchChildren(path, callback);
                        }
                    });
                    callback.onChildrenChanged(newChildren);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error processing children changed event", e);
                }
            }
        });
        callback.onChildrenChanged(children);
    } catch (Exception e) {
        LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
    }
}

public void watchNode(String path, NodeCallback callback) {
    try {
        zooKeeper.exists(path, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeDeleted ||
            event.getType() == Watcher.Event.EventType.NodeDataChanged ||
            event.getType() == Watcher.Event.EventType.NodeCreated) {
                callback.onNodeChanged();
            }
        });
    } catch (Exception e) {
        LOGGER.log(Level.SEVERE,  "Failed to watch node: " + path, e);
    }
}

@Override
public void process(WatchedEvent event) {
    if(event.getState() == Event.KeeperState.SyncConnected) {
connectedSignal.countDown();
LOGGER.info("Connected to ZooKeeper");
    } else if (event.getState() == Event.KeeperState.Disconnected) {
        LOGGER.warning("Disconnected from ZooKeeper");
    } else if (event.getState() == Event.KeeperState.Expired) {
        LOGGER.warning("ZooKeeper session expired, reconnecting...");
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
            connectedSignal = new countDownLatch(1);
            zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
            connectedSignal.await();
            LOGGER.info("Reconnected to ZooKeeper after session expiry");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e)
        }
    }
}

String brokerData = "{\\"host\\":\\"localhost\\",\\"port\\":9092,\\"endpoints\\":[]}";createEphemeralNode("/brokers/ids/1", brokerData);
String topicConfig = "{\\"partitions\\":3,\\"replication_factor\\":2}"; createPersistentNode("/topics/my-topic", topicConfig);

// For partition 0 with replicas on brokers 1,2
createPersistentNode("/topics/my-topic/partitions/0/state", "{\\"leader\\":1,\\"isr\\":[1,2],\\"replicas\\":[1,2]}");

boolean becameController = createEphemeralNode("/controller", "{\\"broker\\":1,\\"timestamp\\":123456789}");

watchNode("/controller", () -> handleControllerChange());

watchChildren("/brokers/ids", children -> { // Handle broker list changes     // If previously known broker missing, initiate leader elections 
});

watchNode("/topics/my-topic/partitions/0/state", () -> { String state = getData("/topics/my-topic/partitions/0/state"); //Parse state JSON and check if this broker is the leader
});