# Simple Kafka

A hands-on implementation of a Kafka-inspired distributed message broker, built in Java to explore the internal architecture of distributed messaging systems.

The project is an educational implementation focused on understanding how brokers, partitions, client-broker communication, binary protocols, and ZooKeeper-based coordination fit together.

> **Project status:** Work in progress. Several core operations are currently scaffolding or placeholders. This project is not a production-ready replacement for Apache Kafka.

## Overview

Apache Kafka is a distributed event-streaming platform built around topics, partitions, brokers, and append-only logs. This project recreates parts of that architecture from the ground up, with an emphasis on learning through implementation.

The current codebase establishes the building blocks for:

- A TCP-based broker server.
- A client API for sending and fetching messages.
- A custom binary protocol for client-broker requests.
- Partition abstractions and offset tracking.
- ZooKeeper connectivity and coordination primitives.
- Broker metadata and controller-election scaffolding.

The goal is to progressively implement and understand the mechanisms that make distributed messaging systems work.

## Architecture

At a high level, the intended architecture looks like this:

```text
                   +----------------------+
                   |     Kafka Client     |
                   |  SimpleKafkaClient   |
                   +----------+-----------+
                              |
                       TCP / Binary Protocol
                              |
              +---------------v----------------+
              |          Broker Server         |
              |       SimpleKafkaBroker        |
              |                                |
              |  +--------------------------+  |
              |  |     Request Handling     |  |
              |  | Produce / Fetch / Metadata| |
              |  +--------------------------+  |
              |                                |
              |  +--------------------------+  |
              |  |       Partitions         |  |
              |  |  Offsets / Log Segments   |  |
              |  +--------------------------+  |
              +---------------+----------------+
                              |
                    Coordination / Metadata
                              |
                    +---------v----------+
                    |      ZooKeeper     |
                    | Broker / Controller|
                    |     Coordination  |
                    +--------------------+
```

This diagram represents the intended component relationships; several request-handling and coordination paths remain to be implemented.

## Technology Stack

| Technology | Purpose |
|---|---|
| Java 11 | Core implementation language and compilation target |
| Maven | Dependency and build management |
| Apache ZooKeeper 3.8.1 | Coordination and metadata primitives |
| Java NIO | TCP networking through channels and buffers |
| Java Concurrency API | Thread pools, concurrent maps, locks, and atomic values |
| GitHub Actions | Automated Maven build |

## Project Structure

```text
kafka/
├── .github/
│   └── workflows/
│       └── maven.yml
├── src/
│   └── main/
│       └── java/
│           └── com/
│               └── simplekafka/
│                   ├── broker/
│                   │   ├── BrokerInfo.java
│                   │   ├── Partition.java
│                   │   ├── Protocol.java
│                   │   ├── SimpleKafkaBroker.java
│                   │   └── ZookeeperClient.java
│                   └── client/
│                       └── SimpleKafkaClient.java
└── pom.xml
```

### Core Components

#### 1. SimpleKafkaBroker

The broker is the server-side entry point.

It currently establishes a TCP server using Java NIO, accepts client connections, and dispatches incoming requests based on their protocol message type.

The broker also contains scaffolding for:

- Broker registration.
- Cluster metadata management.
- Controller election.
- Topic creation and loading.
- Produce and fetch request handling.
- Follower replication.
- Metadata and topic notifications.

Most of these operations are currently placeholders.

#### 2. SimpleKafkaClient

The client provides a starting point for application-to-broker communication.

Its public API includes:

- `initialize()` — initializes client metadata.
- `refreshMetadata()` — intended to retrieve cluster metadata.
- `send(topic, partition, message)` — constructs and sends a produce request.
- `fetch(topic, partition, offset, maxBytes)` — constructs and sends a fetch request.

The request-writing path is present, but response parsing, metadata retrieval, and complete produce/fetch functionality still need implementation.

#### 3. Partition

The partition class provides the foundation for message organization and offset management.

It currently includes:

- Partition identifiers.
- Leader information.
- Follower identifiers.
- Atomic offset tracking.
- Read/write locking.
- A partition data-directory initializer.
- Initial log-segment abstractions.

The persistent append-only log, indexing, segment management, and message retrieval are not yet implemented.

#### 4. Protocol

The protocol class defines message types and request encoders.

Current request types:

| Message | Identifier | Purpose |
|---|---:|---|
| Produce | `0x01` | Send a message to a partition |
| Fetch | `0x02` | Retrieve messages from an offset |
| Metadata | `0x03` | Request broker/topic metadata |
| Create Topic | `0x04` | Request topic creation |
| Replicate | `0x05` | Replicate a message to a follower |
| Topic Notification | `0x06` | Notify about topic changes |

The protocol also defines produce and fetch response identifiers and an error response format.

#### 5. ZookeeperClient

The ZooKeeper client wraps the Apache ZooKeeper Java API.

It provides foundational methods for:

- Connecting to a ZooKeeper server.
- Creating persistent and ephemeral znodes.
- Checking whether a node exists.
- Reading and deleting node data.
- Watching node and child changes.
- Handling connection-state events and session expiration.

The broker's integration with these coordination primitives is still incomplete.

## Getting Started

### Prerequisites

Make sure you have the following installed:

- Java Development Kit (JDK) 11 or later.
- Apache Maven 3.6 or later.
- Apache ZooKeeper 3.8.x, for running the coordination service.

Check your Java and Maven installations:

```bash
java -version
mvn -version
```

### 1. Clone the repository

```bash
git clone https://github.com/AtharvaChitnis/kafka.git
cd kafka
```

### 2. Build the project

Compile and package the application using Maven:

```bash
mvn clean package
```

The Maven Shade Plugin is configured to produce a packaged JAR with dependencies.

### 3. Start ZooKeeper

The project expects a ZooKeeper instance on `localhost` and accepts its port through the broker constructor.

Start a local ZooKeeper server using your ZooKeeper installation. For example, with a configured standalone installation:

```bash
zkServer.sh start
```

The exact startup command may vary depending on how ZooKeeper is installed and configured.

### 4. Broker startup

The broker constructor accepts four parameters:

```java
new SimpleKafkaBroker(brokerId, host, port, zkPort);
```

For example:

```java
SimpleKafkaBroker broker =
    new SimpleKafkaBroker(1, "127.0.0.1", 9092, 2181);

broker.start();
```

The repository currently does not provide a command-line entry point or argument parser for configuring and launching a broker. The snippet above illustrates the constructor and startup API; it is not a preconfigured CLI command.

## Example Client API

The client API is designed to look like this:

```java
SimpleKafkaClient client =
    new SimpleKafkaClient("127.0.0.1", 9092);

client.initialize();

// Send a message
client.send(
    "orders",
    0,
    "Order created".getBytes()
);

// Fetch messages
client.fetch(
    "orders",
    0,
    0L,
    1024
);
```

**Note:** This demonstrates the intended API, not a verified end-to-end messaging workflow. The current broker handlers and client response processing contain placeholders, so successful message production and consumption are not yet supported.

## Build and CI

The repository includes a GitHub Actions workflow at `.github/workflows/maven.yml`.

It runs on pushes and pull requests targeting `main`, sets up Temurin JDK 17, and executes:

```bash
mvn -B clean verify
```

The Maven project targets Java 11 source and bytecode compatibility.

## Learning Objectives

This project is intended to help explore several important distributed-systems concepts:

- **Message brokers:** How clients communicate with a broker over TCP.
- **Binary protocols:** How application-level messages are encoded and decoded.
- **Partitioning:** How a topic can be divided into independently managed partitions.
- **Offsets:** How messages can be addressed and consumed sequentially.
- **Log storage:** How append-only logs and segment files can provide persistent message storage.
- **Replication:** How leaders and followers can maintain copies of partition data.
- **Leader election:** How a distributed coordination service can help select a controller.
- **Concurrency:** How locks, atomic counters, and thread pools coordinate concurrent operations.

## Current Limitations

The project is actively being developed. The following areas require further work:

- [ ] Complete broker registration and cluster metadata management.
- [ ] Implement topic creation and partition assignment.
- [ ] Implement persistent message append and retrieval.
- [ ] Implement log segments and offset indexes.
- [ ] Complete produce and fetch request handlers.
- [ ] Implement client response parsing and error handling.
- [ ] Implement leader/follower replication.
- [ ] Complete controller election and failover handling.
- [ ] Add integration tests with a running ZooKeeper instance.
- [ ] Add multi-broker testing and failure-recovery scenarios.
- [ ] Add configuration options for broker ID, host, ports, and data directories.

## Contributing

Contributions, suggestions, and discussions are welcome.

1. Fork the repository.
2. Create a feature branch.
3. Make your changes.
4. Build the project with `mvn clean verify`.
5. Open a pull request describing the changes.

For larger changes, opening an issue first is a good way to discuss the proposed design.

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache ZooKeeper Documentation](https://zookeeper.apache.org/doc/current/)
- [Java NIO Documentation](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/package-summary.html)
- [Apache Maven Documentation](https://maven.apache.org/guides/)

## Author

**Atharva Chitnis**

- GitHub: [@AtharvaChitnis](https://github.com/AtharvaChitnis)
- Repository: [github.com/AtharvaChitnis/kafka](https://github.com/AtharvaChitnis/kafka)

---

Built as a learning project to understand the internals of distributed messaging systems by implementing the building blocks from scratch.
