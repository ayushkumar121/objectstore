package objectstore;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class BucketPerformanceTest {
    private static final String TEST_BUCKET_NAME = "performance_test_bucket.dat";
    private static Bucket bucket;

    public static void main(String[] args) {
        try {
            setup();
            testLargeFileUpload();
            testMultipleBlobUploads();
            testBlobRetrievalPerformance();
            testBlobDeletionPerformance();
            cleanup();
        } catch (Exception e) {
            System.err.println("Performance test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void setup() throws IOException {
        System.out.println("Setting up performance tests...");
        bucket = new Bucket(TEST_BUCKET_NAME, true);
    }

    private static void cleanup() {
        System.out.println("Cleaning up performance test files...");
        File file = new File(TEST_BUCKET_NAME);
        if (file.exists() && file.delete()) {
            System.out.println("Performance test bucket file deleted successfully.");
        } else {
            System.err.println("Failed to delete performance test bucket file.");
        }
    }

    private static void testLargeFileUpload() throws IOException {
        System.out.println("Running testLargeFileUpload...");

        byte[] largeContent = new byte[1000 * 1024 * 1024]; //1000MB file
        new Random().nextBytes(largeContent);
        String blobName = "largeBlob";

        long startTime = System.currentTimeMillis();
        long offset = bucket.uploadBlob(blobName, largeContent);
        long endTime = System.currentTimeMillis();

        if (offset > 0) {
            System.out.println("Large file uploaded successfully in " + (endTime - startTime) + " ms.");
        } else {
            System.err.println("Large file upload failed.");
        }
    }

    private static void testMultipleBlobUploads() throws IOException {
        System.out.println("Running testMultipleBlobUploads...");

        int numBlobs = 100;
        int blobSize = 1024 * 10; // 10 KB blobs
        Random random = new Random();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numBlobs; i++) {
            byte[] content = new byte[blobSize];
            random.nextBytes(content);
            bucket.uploadBlob("blob_" + i, content);
        }
        long endTime = System.currentTimeMillis();

        System.out.println("Uploaded " + numBlobs + " blobs in " + (endTime - startTime) + " ms.");
    }

    private static void testBlobRetrievalPerformance() throws IOException {
        System.out.println("Running testBlobRetrievalPerformance...");

        byte[] content = new byte[1024 * 1024]; // 1 MB blob
        new Random().nextBytes(content);
        String blobName = "retrievalBlob";
        long offset = bucket.uploadBlob(blobName, content);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            Blob blob = bucket.getBlob(offset);
            if (blob == null || blob.content().length != content.length) {
                System.err.println("Blob retrieval failed at iteration " + i);
            }
        }
        long endTime = System.currentTimeMillis();

        System.out.println("Retrieved 100 blobs in " + (endTime - startTime) + " ms.");
    }

    private static void testBlobDeletionPerformance() throws IOException {
        System.out.println("Running testBlobDeletionPerformance...");

        int numBlobs = 50;
        int blobSize = 1024 * 5; // 5 KB blobs
        long[] offsets = new long[numBlobs];
        Random random = new Random();

        // Upload blobs first
        for (int i = 0; i < numBlobs; i++) {
            byte[] content = new byte[blobSize];
            random.nextBytes(content);
            offsets[i] = bucket.uploadBlob("deleteBlob_" + i, content);
        }

        long startTime = System.currentTimeMillis();
        for (long offset : offsets) {
            bucket.deleteBlob(offset);
        }
        long endTime = System.currentTimeMillis();

        System.out.println("Deleted " + numBlobs + " blobs in " + (endTime - startTime) + " ms.");
    }
}
