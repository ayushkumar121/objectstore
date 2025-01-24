public class Blob {
    private String path;
    private byte[] content;

    public Blob(String path, byte[] content) {
        this.path = path;
        this.content = content;
    }

    public String getPath() {
        return path;
    }
    public byte[] getContent() {
        return content;
    }
}
