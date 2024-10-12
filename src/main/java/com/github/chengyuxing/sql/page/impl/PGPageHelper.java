package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

/**
 * <p>PostgreSQL page helper, e.g.</p>
 * <blockquote>
 * <pre>select * from ... limit :{@link  #START_NUM_KEY} offset :{@link #END_NUM_KEY};</pre>
 * </blockquote>
 *
 * @see #pagedArgs()
 */
public class PGPageHelper extends PageHelper {

    public int limit() {
        return pageSize;
    }

    public int offset() {
        if (recordCount == 0) {
            return 0;
        }
        return (pageNumber - 1) * pageSize;
    }

    @Override
    public String pagedSql(String sql) {
        return sql + " LIMIT :" + START_NUM_KEY + " OFFSET :" + END_NUM_KEY;
    }

    @Override
    public Args<Integer> pagedArgs() {
        return Args.of(START_NUM_KEY, limit()).add(END_NUM_KEY, offset());
    }
}
