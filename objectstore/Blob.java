package objectstore;

public record Blob(String name, byte[] content) {
    public int getBlobSize() {
        return 2 + name.length() + 4 + content.length;
    }
}
