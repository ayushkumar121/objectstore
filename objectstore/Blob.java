package objectstore;

public record Blob(String path, byte[] content) {
    public int getBlobSize() {
        return 2 + path.length() + 4 + content.length;
    }
}
