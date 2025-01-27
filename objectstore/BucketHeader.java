package objectstore;

public class BucketHeader {
    public final int bucketHeaderSize;
    public final int pageSize;
    public final int pageTableSize;
    public long blobEntriesOffset;
    public final byte[] pageTable;

    public BucketHeader() {
        this.bucketHeaderSize = 4 + 4 + 4 + 8 + Page.PAGE_TABLE_SIZE;
        this.pageSize = Page.PAGE_SIZE;
        this.pageTableSize = Page.PAGE_TABLE_SIZE;
        this.blobEntriesOffset = bucketHeaderSize; // Initial offset
        this.pageTable = new byte[this.pageTableSize];
    }

    public BucketHeader(int pageSize, int pageTableSize, byte[] pageTable, int blobEntriesOffset) {
        this.bucketHeaderSize = 4 + 4 + 4 + 4 + pageSize;
        this.pageSize = pageSize;
        this.pageTableSize = pageTableSize;
        this.blobEntriesOffset = blobEntriesOffset;
        this.pageTable = pageTable;
    }
}
