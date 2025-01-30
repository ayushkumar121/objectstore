package objectstore;

import java.io.File;
import java.io.IOException;

public class BucketTest {

    private static final String TEST_BUCKET_NAME = "test_bucket.dat";
    private static Bucket bucket;

    public static void main(String[] args) {
        try {
            setup();
            testBucketCreation();
            testUploadBlob();
            testRetrieveBlob();
            testDeleteBlob();
            testDuplicateBlobUpload();
            testPageAllocation();
            testBlobEntriesPersistency();
            cleanup();
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void setup() throws IOException {
        System.out.println("Setting up tests...");
        bucket = new Bucket(TEST_BUCKET_NAME, true);
    }

    private static void cleanup() {
        System.out.println("Cleaning up test files...");
        File file = new File(TEST_BUCKET_NAME);
        if (file.exists() && file.delete()) {
            System.out.println("Test bucket file deleted successfully.");
        } else {
            System.out.println("Failed to delete test bucket file.");
        }
    }

    private static void testBucketCreation() {
        System.out.println("Running testBucketCreation...");
        if (bucket != null) {
            System.out.println("Bucket created successfully.");
        } else {
            System.err.println("Bucket creation failed.");
        }
    }

    private static void testUploadBlob() throws IOException {
        System.out.println("Running testUploadBlob...");
        String blobName = "testBlob";
        byte[] content = "Hello, world!".getBytes();

        bucket.uploadBlob(blobName, content);
        if (bucket.getBlobs().containsKey(blobName)) {
            System.out.println("Blob uploaded successfully.");
        } else {
            System.err.println("Blob upload failed.");
        }
    }

    private static void testRetrieveBlob() throws IOException {
        System.out.println("Running testRetrieveBlob...");
        String blobName = "retrieveBlob";
        byte[] content = "Retrieve Me".getBytes();

        bucket.uploadBlob(blobName, content);
        Blob retrievedBlob = bucket.getBlob(blobName);

        if (blobName.equals(retrievedBlob.name()) && java.util.Arrays.equals(content, retrievedBlob.content())) {
            System.out.println("Blob retrieved successfully.");
        } else {
            System.err.println("Blob retrieval failed.");
        }
    }

    private static void testDeleteBlob() throws IOException {
        System.out.println("Running testDeleteBlob...");
        String blobName = "deleteBlob";
        byte[] content = "Delete Me".getBytes();

        bucket.uploadBlob(blobName, content);
        bucket.deleteBlob(blobName);

        if (!bucket.getBlobs().containsKey(blobName)) {
            System.out.println("Blob deleted successfully.");
        } else {
            System.err.println("Blob deletion failed.");
        }
    }

    private static void testDuplicateBlobUpload() {
        System.out.println("Running testDuplicateBlobUpload...");
        String blobName = "duplicateBlob";
        byte[] content = "Duplicate Content".getBytes();

        try {
            bucket.uploadBlob(blobName, content);
            bucket.uploadBlob(blobName, content);
            System.err.println("Duplicate blob should have thrown an exception.");
        } catch (RuntimeException e) {
            if ("blob already exists".equals(e.getMessage())) {
                System.out.println("Duplicate blob upload test passed.");
            } else {
                System.err.println("Unexpected exception message: " + e.getMessage());
            }
        } catch (IOException e) {
            System.err.println("Unexpected IO exception: " + e.getMessage());
        }
    }

    private static void testPageAllocation() throws IOException {
        System.out.println("Running testPageAllocation...");
        byte[] largeContent = new byte[4096 * 3];  // 3 full pages
        String blobName = "largeBlob";

        bucket.uploadBlob(blobName, largeContent);
        System.out.println("Large blob uploaded and allocated successfully.");
    }

    private static void testBlobEntriesPersistency() throws IOException {
        System.out.println("Running testBlobEntriesPersistency...");
        String blobName = "persistBlob";
        byte[] content = "Persistent Data".getBytes();

        bucket.uploadBlob(blobName, content);
        bucket = new Bucket(TEST_BUCKET_NAME, false);  // Reload bucket from disk

        if (bucket.getBlobs().containsKey(blobName)) {
            System.out.println("Blob entries persisted after reload.");
        } else {
            System.err.println("Blob entries did not persist.");
        }
    }
}

