package objectstore;

import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        Bucket b = new Bucket("hello.bucket");
        long offset = b.uploadBlob("hello-world.txt", "Hello World".getBytes());
        b.uploadBlob("foo.txt", "Foodfdf".getBytes());
        b.uploadBlob("bar.txt", "bar".getBytes());

        b.deleteBlob(offset);

        b.uploadBlob("baz.txt", "baz".getBytes());

        var blobs = b.getBlobs();
        for (Map.Entry<String, Long> blobEntry : blobs.entrySet()) {
            var blob = b.getBlob(blobEntry.getValue());
            System.out.println(blob.name());
            System.out.println(blob.content().length);
        }
    }
}
