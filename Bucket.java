import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Bucket {
    private File file;

    public Bucket(String name) throws RuntimeException, FileNotFoundException, IOException {
        if (!isValidBucketName(name)) {
            throw new RuntimeException("invalid name");
        }
        this.file = new File(name);
        try(FileOutputStream _ = new FileOutputStream(file)) {}
    }

    public Blob GetBlob(long offset) throws Exception {
        try (FileInputStream in = new FileInputStream(file)) {
            in.skip(offset);
            int pathLength = decodeInt(in.readNBytes(4));
            byte[] pathBug = in.readNBytes(pathLength);

            int contentLength = decodeInt(in.readNBytes(4));
            byte[] contentBuf = in.readNBytes(contentLength);

            return new Blob(new String(pathBug), contentBuf);
        }
    }

    public long UploadBlob(String path, byte[] content) throws IOException {
        try (FileOutputStream out = new FileOutputStream(file, true)) {
            long offset = file.length();
            FileChannel ch = out.getChannel();
            ch.position(offset);
            // Writing path 
            ch.write(ByteBuffer.wrap(encodeInt(path.length())));
            ch.write(ByteBuffer.wrap(path.getBytes()));

            // Writing content
            ch.write(ByteBuffer.wrap(encodeInt(content.length)));
            ch.write(ByteBuffer.wrap(content));
            return offset;
        }
    }

    private boolean isValidBucketName(String name) {
        return !(name.contains("/") || name.contains("\\"));
    }

    // We're using BigEndian encoding
    private byte[] encodeInt(int i) {
        byte[] buf = new byte[4];
        buf[0] = (byte)(i >> 24);
        buf[1] = (byte)(i >> 16);
        buf[2] = (byte)(i >> 8);
        buf[3] = (byte)(i);
        return buf;
    }

    private int decodeInt(byte[] buf) {
        assert buf.length == 4;
        return (buf[0] << 24) | (buf[1] << 16) |  (buf[2] << 8) | buf[3];
    }
}