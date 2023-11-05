package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

/**
 * <p>PostgreSQL page helper.</p>
 * Default paged args: {@code :limit, :offset}<br>
 * e.g.
 * <blockquote>
 * <pre>select * from ... limit :limit offset :offset;</pre>
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
        return sql + " LIMIT " + limit() + " OFFSET " + offset();
    }

    @Override
    public Args<Integer> pagedArgs() {
        return Args.of("limit", limit()).add("offset", offset());
    }
}
