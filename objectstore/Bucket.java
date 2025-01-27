package objectstore;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Bucket {
    private final File file;
    private final BucketHeader bucketHeader;
    private final Map<String, Long> blobEntries;

    public Bucket(String bucketName) throws IOException {
        this(bucketName, false);
    }

    public Bucket(String bucketName, boolean override) throws IOException {
        this.file = new File(bucketName);
        this.blobEntries  = new HashMap<>();

        try (RandomAccessFile rw = new RandomAccessFile(this.file, "rw")) {
            // Reading existing bucket
            if (!override && this.file.exists()) {
                int bucketHeaderSize = rw.readInt();
                int pageSize = rw.readInt();
                int pageTableSize = rw.readInt();
                long blobEntriesOffset = rw.readLong();
                byte[] pageTable = new byte[pageTableSize];
                rw.read(pageTable);
                this.bucketHeader = new BucketHeader(pageSize, pageTableSize, blobEntriesOffset, pageTable);
                assert bucketHeaderSize == this.bucketHeader.bucketHeaderSize;

                rw.seek(blobEntriesOffset);
                int blobEntriesCount = rw.readInt();

                for (int i = 0; i < blobEntriesCount; i++) {
                    String name = rw.readUTF();
                    long offset = rw.readLong();
                    this.blobEntries.put(name, offset);
                }
            } else {
                this.bucketHeader = new BucketHeader();

                // Write initial header
                rw.writeInt(this.bucketHeader.bucketHeaderSize);
                rw.writeInt(this.bucketHeader.pageSize);
                rw.writeInt(this.bucketHeader.pageTableSize);
                rw.writeLong(this.bucketHeader.blobEntriesOffset);
                rw.write(this.bucketHeader.pageTable);
            }
        }
    }

    public Map<String, Long> getBlobs() {
        return blobEntries;
    }

    public Blob getBlob(long offset) throws IOException {
        try (RandomAccessFile in = new RandomAccessFile(file, "r")) {
            in.seek(offset);

            String path = in.readUTF();

            int contentLength = in.readInt();
            byte[] contentBuf = new byte[contentLength];

            return new Blob(path, contentBuf);
        }
    }

    public BlobMetadata getBlobMetadata(long offset) throws IOException {
        try (RandomAccessFile in = new RandomAccessFile(file, "r")) {
            in.seek(offset);

            String name = in.readUTF();
            int contentLength = in.readInt();

            return new BlobMetadata(name, contentLength);
        }
    }

    public void deleteBlob(long offset) throws IOException {
        BlobMetadata m = getBlobMetadata(offset);
        PageRange pages = offsetToPages(offset, m.blobSize());

        try (RandomAccessFile rw = new RandomAccessFile(file, "rw")) {
            // Updating page usage
            rw.seek((this.bucketHeader.bucketHeaderSize -
                    (long) this.bucketHeader.pageTableSize) + pages.start());
            for (int i = pages.start(); i < pages.start()+pages.count(); i++) {
                this.bucketHeader.pageTable[i] = Page.PAGE_FREE;
                rw.write(Page.PAGE_FREE);
            }

            this.blobEntries.remove(m.name());
            writeUpdatedBlobEntries(rw);
        }
    }

    public long uploadBlob(String name, byte[] content) throws RuntimeException, IOException {
        if (this.blobEntries.get(name) != null) {
            throw new RuntimeException("blob already exists");
        }

        int totalBlobSize = 2 + name.length() + 4 + content.length;
        PageRange pages = findUnusedPages(totalBlobSize);

        try (RandomAccessFile rw = new RandomAccessFile(this.file, "rw")) {
            // Writing new offset for blob entries
            rw.seek(this.bucketHeader.bucketHeaderSize - (long) (this.bucketHeader.pageTableSize + 8));
            this.bucketHeader.blobEntriesOffset = Math.max(
                    this.bucketHeader.blobEntriesOffset,
                    this.bucketHeader.bucketHeaderSize + (long) (pages.start() + pages.count()) * Page.PAGE_SIZE);
            rw.writeLong(this.bucketHeader.blobEntriesOffset);

            // Updating page usage
            rw.skipBytes(pages.start());
            for (int i = pages.start(); i < pages.start()+pages.count(); i++) {
                this.bucketHeader.pageTable[i] = Page.PAGE_USED;
                rw.write(Page.PAGE_USED);
            }

            long offset = this.bucketHeader.bucketHeaderSize + (long) pages.start() * Page.PAGE_SIZE;
            rw.seek(offset);

            rw.writeUTF(name);

            rw.writeInt(content.length);
            rw.write(content);

            this.blobEntries.put(name, offset);
            writeUpdatedBlobEntries(rw);

            return offset;
        }
    }

    private PageRange findUnusedPages(int blobSize) {
        int count = (blobSize + this.bucketHeader.pageSize) / this.bucketHeader.pageSize;
        int start = -1;
        for (int i = 0; i < this.bucketHeader.pageTableSize; i++) {
            boolean found = true;
            for (int j = i; j < i+count; j++) {
                if(this.bucketHeader.pageTable[j] == Page.PAGE_USED) {
                    found = false;
                    break;
                }
            }
            if (found) {
                start = i;
                break;
            }
        }
        assert start != -1;
        return new PageRange(start, count);
    }

    private PageRange offsetToPages(long offset, int blobSize) {
        int start = (int)((offset - this.bucketHeader.bucketHeaderSize) / Page.PAGE_SIZE);
        int count = (blobSize + this.bucketHeader.pageSize) / this.bucketHeader.pageSize;

        return new PageRange(start, count);
    }

    private void writeUpdatedBlobEntries(RandomAccessFile out) throws IOException {
        out.setLength(this.bucketHeader.blobEntriesOffset);
        out.seek(this.bucketHeader.blobEntriesOffset);
        out.writeInt(this.blobEntries.size());
        for (Map.Entry<String, Long> e: this.blobEntries.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeLong(e.getValue());
        }
    }
}