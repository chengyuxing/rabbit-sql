package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

/**
 * <p>Mysql/MariaDB page helper.</p>
 * Default paged args: {@link  #START_NUM_KEY}, {@link #END_NUM_KEY}<br>
 * e.g.
 * <blockquote>
 * <pre>select * from ... limit :{@link  #START_NUM_KEY}, :{@link #END_NUM_KEY};</pre>
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
        return Args.of(START_NUM_KEY, start()).add(END_NUM_KEY, size());
    }
}
