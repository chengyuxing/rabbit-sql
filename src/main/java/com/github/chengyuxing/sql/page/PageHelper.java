package com.github.chengyuxing.sql.page;

import com.github.chengyuxing.sql.Args;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Range;

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
    public abstract @NotNull String pagedSql(char namedParamPrefix, @NotNull String sql);

    /**
     * Paged args.
     *
     * @return paged args
     * @see #START_NUM_KEY
     * @see #END_NUM_KEY
     */
    public abstract @NotNull Args<Integer> pagedArgs();

    /**
     * Create count query sql.
     *
     * @param sql record query sql
     * @return count query sql
     */
    public String countSql(@NotNull String sql) {
        return "select count(*) from (\n" + sql + "\n) t_4_rabbit";
    }

    public @Range(from = 0, to = Integer.MAX_VALUE) int getPageCount() {
        return pageCount;
    }

    public @Range(from = 1, to = Integer.MAX_VALUE) int getPageNumber() {
        return pageNumber;
    }

    public @Range(from = 1, to = Integer.MAX_VALUE) int getPageSize() {
        return pageSize;
    }

    public @Range(from = 0, to = Integer.MAX_VALUE) int getRecordCount() {
        return recordCount;
    }

    /**
     * Initial page helper with page, size and record count.
     *
     * @param page  current page
     * @param size  page size
     * @param count record count
     */
    public void init(@Range(from = 1, to = Integer.MAX_VALUE) int page,
                     @Range(from = 1, to = Integer.MAX_VALUE) int size,
                     @Range(from = 0, to = Integer.MAX_VALUE) int count) {
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
