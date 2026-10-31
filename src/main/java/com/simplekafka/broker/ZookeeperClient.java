package com.simplekafka.broker;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZookeeperClient implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 30000;

    private final String host;
    private final int port;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal;

    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException, KeeperException, InterruptedException {
        connectedSignal = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();
        createPersistentNode("/brokers", "");
        createPersistentNode("/topics", "");
        createPersistentNode("/controller", "");
    }

    public String getConnectString() {
        return host + ":" + port;
    }

    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        if (zooKeeper == null) {
            return;
        }
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, data.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        if (zooKeeper == null) {
            return false;
        }
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true;
        }
        LOGGER.info("Ephemeral node already exists: " + path);
        return false;
    }

    public boolean exists(String path) throws KeeperException, InterruptedException {
        return zooKeeper != null && zooKeeper.exists(path, false) != null;
    }

    public String getData(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            return null;
        }
        byte[] data = zooKeeper.getData(path, false, stat);
        return data == null ? null : new String(data);
    }

    public void deleteNode(String path) throws KeeperException, InterruptedException {
        if (zooKeeper != null && zooKeeper.exists(path, false) != null) {
            zooKeeper.delete(path, -1);
        }
    }

    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    watchChildren(path, callback);
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
                if (event != null && (event.getType() == Watcher.Event.EventType.NodeDeleted
                        || event.getType() == Watcher.Event.EventType.NodeDataChanged
                        || event.getType() == Watcher.Event.EventType.NodeCreated)) {
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    @Override
    public void process(org.apache.zookeeper.WatchedEvent event) {
        if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
            if (connectedSignal != null) {
                connectedSignal.countDown();
            }
            LOGGER.info("Connected to ZooKeeper");
        } else if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.Disconnected) {
            LOGGER.warning("Disconnected from ZooKeeper");
        } else if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallback {
        void onNodeChanged();
    }
}
