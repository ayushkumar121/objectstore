package objectstore;

public class Page {
    public static int PAGE_SIZE = 1024;
    public static int PAGE_TABLE_SIZE = 100;

    public static byte PAGE_UNCOMMITED = 0;
    public static byte PAGE_FREE = 1;
    public static byte PAGE_USED = 2;
}
