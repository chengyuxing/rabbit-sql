package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

/**
 * <p>Mysql/MariaDB page helper.</p>
 * Default paged args: {@code :limit, :size}<br>
 * e.g.
 * <blockquote>
 * <pre>select * from ... limit :limit, :size;</pre>
 * </blockquote>
 *
 * @see #pagedArgs()
 */
public class MysqlPageHelper extends PageHelper {

    public int start() {
        return (pageNumber - 1) * pageSize;
    }

    public int size() {
        if (recordCount == 0) {
            return 0;
        }
        return pageSize;
    }

    @Override
    public String pagedSql(String sql) {
        return sql + " limit " + start() + ", " + size();
    }

    @Override
    public Args<Integer> pagedArgs() {
        return Args.of("limit", start()).add("size", size());
    }
}
