package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;
import org.jetbrains.annotations.NotNull;

/**
 * <p>Mysql/MariaDB page helper,
 * e.g.</p>
 * <blockquote>
 * <pre>select * from ... limit :{@link  #START_NUM_KEY}, :{@link #END_NUM_KEY};</pre>
 * </blockquote>
 *
 * @see #pagedArgs()
 */
public class MysqlPageHelper extends PageHelper {

    public int limit() {
        return (pageNumber - 1) * pageSize;
    }

    public int size() {
        if (recordCount == 0) {
            return 0;
        }
        return pageSize;
    }

    @Override
    public @NotNull String pagedSql(char namedParamPrefix, @NotNull String sql) {
        return sql + "\nlimit " + namedParamPrefix + START_NUM_KEY + ", " + namedParamPrefix + END_NUM_KEY;
    }

    @Override
    public @NotNull Args<Integer> pagedArgs() {
        return Args.of(START_NUM_KEY, limit()).add(END_NUM_KEY, size());
    }
}
