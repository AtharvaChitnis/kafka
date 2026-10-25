private final int id; //Unique partition identifier
private int leader; //Leader broker ID
private List<Integer> followers; //follower broker IDs for replication
private String baseDir; //Directory for log storage
private final AtomicLong nextOffset;  //next available message offset
private final ReadWriteLock lock; //Concurrency control mechanism
private RandomAccessFile activeLogFile; //Currently active log file
private FileChannel activeLogChannel; //Channel for file operations
private final List<SegmentInfo> segments; //List of segments in the partition 

private void initialize() {
/**
 * Create directory if needed
 * Load existing segments
 * Sort segments by offset
 * Determine next available offset
 * Create a new segment if none exists
 * Open the last segment for appending
 */
}

public long append(byte[] message){
    /**
     * Acquire write lock
     * Check if new segmetn needed
     * Write message size and data
     * Force write to disk
     * Update index
     * Increment offset counter
     * Return assigned offset
     */
}

public List<byte[]> readMessages(long offset, int maxBytes) {
    /**
     * Acquire read lock
     * Find segment containing offset
     * Find file position using index
     * REad messages until maxBytes limit
     * Handle segment boundaries
     * Return message list
     */
}

private void createNewSegment(long, baseOffset)
private void openSegmetnForAppend(SegmentInfo segment)
private SegmentInfo findSegmentForOffset(long offset)
private long findPositionForOffset(SegmentInfo segment, long offset)

private void updateIndex(long offset, long position)
private long findPostionForOffset(SegmentInfo segment, long offset)

