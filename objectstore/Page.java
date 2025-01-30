package objectstore;

public class Page {
    public static final int PAGE_SIZE = 1024;
    public static final int PAGE_TABLE_SIZE = 5*1024;

    public static final byte PAGE_UNCOMMITED = 0;
    public static final byte PAGE_FREE = 1;
    public static final byte PAGE_USED = 2;

    private Page() {}
}
