public class Main {
    public static void main(String[] args) throws Exception {
        Bucket b = new Bucket("hello.bucket");
        b.UploadBlob("hello-world.txt", "Hello World".getBytes());
        b.UploadBlob("foo.txt", "Foodfdf".getBytes());
        b.UploadBlob("bar.txt", "bar".getBytes());

        var blobs = b.GetBlobs();
        for (BlobEntry blobEntry : blobs) {
            System.out.println(blobEntry.path()+" "+blobEntry.offset());
        }

        var blob = b.GetBlob(34);
        System.out.println(blob.content().length);
    }
}
