package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Partition {
    private final int id;
    private int leader;
    private final List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private final List<SegmentInfo> segments;

    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id = id;
        this.leader = leader;
        this.followers = new ArrayList<>(followers);
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0L);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
        initialize();
    }

    private void initialize() {
        File dir = new File(baseDir);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("Unable to create partition directory: " + baseDir);
        }
    }

    public long append(byte[] message) {
        if (message == null) {
            throw new IllegalArgumentException("message cannot be null");
        }
        lock.writeLock().lock();
        try {
            long offset = nextOffset.getAndIncrement();
            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<byte[]> readMessages(long offset, int maxBytes) {
        lock.readLock().lock();
        try {
            List<byte[]> result = new ArrayList<>();
            if (offset < 0 || maxBytes <= 0) {
                return result;
            }
            result.add(new byte[0]);
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public int getLeader() {
        return leader;
    }

    public int getId() {
        return id;
    }

    public long getNextOffset() {
        return nextOffset.get();
    }

    private void createNewSegment(long baseOffset) {
    }

    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        if (segment == null) {
            return;
        }
        if (activeLogFile != null) {
            activeLogFile.close();
        }
        activeLogFile = new RandomAccessFile(segment.getPath(), "rw");
        activeLogChannel = activeLogFile.getChannel();
    }

    private SegmentInfo findSegmentForOffset(long offset) {
        return segments.isEmpty() ? null : segments.get(0);
    }

    private long findPositionForOffset(SegmentInfo segment, long offset) {
        return offset;
    }

    private void updateIndex(long offset, long position) {
    }

    private long findPostionForOffset(SegmentInfo segment, long offset) {
        return findPositionForOffset(segment, offset);
    }

    private static final class SegmentInfo {
        private final String path;

        private SegmentInfo(String path) {
            this.path = path;
        }

        public String getPath() {
            return path;
        }
    }
}

