package objectstore;

import javax.print.attribute.standard.PageRanges;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Bucket {
    private final File file;
    private final BucketHeader bucketHeader;
    private final Map<String, Long> blobEntries;

    public Bucket(String name) throws IOException {
        this.file = new File(name);
        this.bucketHeader = new BucketHeader();
        this.blobEntries  = new HashMap<>();

        // Write initial header
        try (RandomAccessFile out = new RandomAccessFile(this.file, "rw")) {
            out.writeInt(this.bucketHeader.bucketHeaderSize);
            out.writeInt(this.bucketHeader.pageSize);
            out.writeInt(this.bucketHeader.pageTableSize);
            out.writeLong(this.bucketHeader.blobEntriesOffset);
            out.write(this.bucketHeader.pageTable);
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

    public void deleteBlob(long offset) throws IOException {
        Blob b = getBlob(offset); // TODO: improve this
        PageRange pages = offsetToPages(offset, b.getBlobSize());

        try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
            // Updating page usage
            out.seek((this.bucketHeader.bucketHeaderSize -
                    (long) this.bucketHeader.pageTableSize) + pages.start());
            for (int i = pages.start(); i < pages.start()+pages.count(); i++) {
                this.bucketHeader.pageTable[i] = Page.PAGE_FREE;
                out.write(Page.PAGE_FREE);
            }

            this.blobEntries.remove(b.path());
            writeBucketFooter(out);
        }
    }

    public long uploadBlob(String path, byte[] content) throws RuntimeException, IOException {
        if (this.blobEntries.get(path) != null) {
            throw new RuntimeException("file already exists");
        }

        int totalBlobSize = 2 + path.length() + 4 + content.length;
        PageRange pages = findUnusedPages(totalBlobSize);

        try (RandomAccessFile out = new RandomAccessFile(this.file, "rw")) {
            // Writing new offset for blob entries
            out.seek(this.bucketHeader.bucketHeaderSize - (long) (this.bucketHeader.pageTableSize + 8));
            this.bucketHeader.blobEntriesOffset = Math.max(
                    this.bucketHeader.blobEntriesOffset,
                    this.bucketHeader.bucketHeaderSize + (long) (pages.start()+1) * Page.PAGE_SIZE);
            out.writeLong(this.bucketHeader.blobEntriesOffset);

            // Updating page usage
            out.skipBytes(pages.start());
            for (int i = pages.start(); i < pages.start()+pages.count(); i++) {
                this.bucketHeader.pageTable[i] = Page.PAGE_USED;
                out.write(Page.PAGE_USED);
            }

            long offset = this.bucketHeader.bucketHeaderSize + (long) pages.start() * Page.PAGE_SIZE;
            out.seek(offset);

            out.writeUTF(path);

            out.writeInt(content.length);
            out.write(content);

            this.blobEntries.put(path, offset);
            writeBucketFooter(out);

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

    private void writeBucketFooter(RandomAccessFile out) throws IOException {
        out.setLength(this.bucketHeader.blobEntriesOffset);
        out.seek(this.bucketHeader.blobEntriesOffset);
        for (Map.Entry<String, Long> e: this.blobEntries.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeLong(e.getValue());
        }
    }
}