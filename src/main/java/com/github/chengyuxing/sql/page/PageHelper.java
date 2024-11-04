package com.github.chengyuxing.sql.page;

import com.github.chengyuxing.sql.Args;

/**
 * Abstract page helper.
 */
public abstract class PageHelper {
    public static final String ROW_NUM_KEY = "rn_4_rabbit";
    public static final String START_NUM_KEY = "start_4_rabbit";
    public static final String END_NUM_KEY = "end_4_rabbit";
    protected int pageNumber;
    protected int pageSize;
    protected int pageCount;
    protected int recordCount;

    /**
     * Create paged sql.
     *
     * @param namedParamPrefix named parameter prefix
     * @param sql              sql
     * @return paged sql
     */
    public abstract String pagedSql(char namedParamPrefix, String sql);

    /**
     * Paged args.
     *
     * @return paged args
     * @see #START_NUM_KEY
     * @see #END_NUM_KEY
     */
    public abstract Args<Integer> pagedArgs();

    public int getPageCount() {
        return pageCount;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getRecordCount() {
        return recordCount;
    }

    /**
     * Initial page helper with page, size and record count.
     *
     * @param page  current page
     * @param size  page size
     * @param count record count
     */
    public void init(int page, int size, int count) {
        pageNumber = page;
        pageSize = size;
        recordCount = count;
        pageCount = recordCount / pageSize;
        if (recordCount % pageSize != 0) {
            pageCount += 1;
        }
    }

    @Override
    public String toString() {
        return "PageHelper{" +
                "pageNumber=" + pageNumber +
                ", pageSize=" + pageSize +
                ", pageCount=" + pageCount +
                ", recordCount=" + recordCount +
                '}';
    }
}
