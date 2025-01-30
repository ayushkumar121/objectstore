package objectstore;

import java.io.*;
import java.util.*;

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
        return Collections.unmodifiableMap(blobEntries);
    }

    public synchronized Blob getBlob(String name) throws IOException {
        Long offset = this.blobEntries.get(name);
        if (offset == null) {
            throw new RuntimeException("blob does not exists exists");
        }

        byte[] content = new byte[0];

        int contentSize = 0;
        try (RandomAccessFile in = new RandomAccessFile(file, "r")) {
            while(offset != 0) {
                in.seek(offset);
                long nextOffset = in.readLong();
                int chunkSize = in.readInt();
                byte[] chunk = new byte[chunkSize];
                in.read(chunk);

                contentSize += chunkSize;
                offset = nextOffset;

                byte[] newContentArray = new byte[contentSize];
                System.arraycopy(content, 0, newContentArray, 0, content.length);
                System.arraycopy(chunk, 0, newContentArray, content.length, chunkSize);

                content = newContentArray;
            }

            return new Blob(name, content);
        }
    }

    public synchronized void deleteBlob(String name) throws IOException {
        Long offset = this.blobEntries.get(name);
        if (offset == null) {
            throw new RuntimeException("blob does not exists exists");
        }

        try (RandomAccessFile rw = new RandomAccessFile(file, "rw")) {
            while(offset != 0) {
                rw.seek(offset);
                long nextOffset = rw.readLong();
                int chunkSize = rw.readInt();
                byte[] chunk = new byte[chunkSize];
                rw.read(chunk);

                int pageIndex = (int) (offset - this.bucketHeader.bucketHeaderSize) / this.bucketHeader.pageSize;
                this.bucketHeader.pageTable[pageIndex] = Page.PAGE_FREE;

                offset = nextOffset;
            }

            this.blobEntries.remove(name);
            writeUpdatedBlobEntries(rw);
        }
    }

    public synchronized void uploadBlob(String name, byte[] content) throws RuntimeException, IOException {
        if (this.blobEntries.get(name) != null) {
            throw new RuntimeException("blob already exists");
        }

        int totalBlobSize = 4 + content.length;
        int pagesCount = (totalBlobSize + this.bucketHeader.pageSize - 1) / this.bucketHeader.pageSize;
        List<Integer> pageIndexes = findUnusedPages(pagesCount);
        if (pageIndexes.size() < pagesCount) {
            throw new RuntimeException("no space left in the bucket");
        }
        Integer minPageIndex = Collections.min(pageIndexes);
        Integer maxPageIndex = Collections.max(pageIndexes);

        try (RandomAccessFile rw = new RandomAccessFile(this.file, "rw")) {
            // Writing new offset for blob entries
            rw.seek(this.bucketHeader.bucketHeaderSize - (long) (this.bucketHeader.pageTableSize + 8));
            this.bucketHeader.blobEntriesOffset = Math.max(
                    this.bucketHeader.blobEntriesOffset,
                    this.bucketHeader.bucketHeaderSize + (long) (maxPageIndex+1) * this.bucketHeader.pageSize);
            rw.writeLong(this.bucketHeader.blobEntriesOffset);

            int written = 0;
            for (int i = 0; i < pageIndexes.size(); i++) {
                Integer pageIndex = pageIndexes.get(i);

                // Updating page usage
                this.bucketHeader.pageTable[pageIndex] = Page.PAGE_USED;

                // Updating page table on disk
                rw.seek(this.bucketHeader.bucketHeaderSize - this.bucketHeader.pageTableSize + (long)pageIndex);
                rw.write(Page.PAGE_USED);

                // Copying blob to page
                long pageOffset = this.bucketHeader.bucketHeaderSize + (long) pageIndex * this.bucketHeader.pageSize;
                rw.seek(pageOffset);

                if (i < pageIndexes.size() - 1) {
                    long nextOffset = this.bucketHeader.bucketHeaderSize + (long) pageIndexes.get(i+1) * this.bucketHeader.pageSize;
                    rw.writeLong(nextOffset);
                } else {
                    rw.writeLong(0);
                }

                int toWrite = Math.min(this.bucketHeader.pageSize - 12, content.length - written);
                rw.writeInt(toWrite);
                rw.write(content, written, toWrite);

                written += toWrite;
            }

            long offset = this.bucketHeader.bucketHeaderSize + (long) minPageIndex * this.bucketHeader.pageSize;

            this.blobEntries.put(name, offset);
            writeUpdatedBlobEntries(rw);
        }
    }

    private List<Integer> findUnusedPages(int pageCount) {
        List<Integer> unusedPages = new ArrayList<>();
        for (int i = 0; i < this.bucketHeader.pageTableSize; i++) {
            if (unusedPages.size() >= pageCount) break;

            if (this.bucketHeader.pageTable[i] != Page.PAGE_USED) {
                unusedPages.add(i);
            }
        }
        return unusedPages;
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