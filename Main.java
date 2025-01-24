public class Main {
    public static void main(String[] args) throws Exception {
        Bucket b = new Bucket("hello.bucket");
        long o1 = b.UploadBlob("hello-world.txt", "Hello World".getBytes());
        long o2 = b.UploadBlob("foo.txt", "Foodfdf".getBytes());
        long o3 = b.UploadBlob("bar.txt", "bar".getBytes());

        Blob blob = b.GetBlob(o2);
        System.out.println(o2 + " "+ blob.getPath());
    }
}
