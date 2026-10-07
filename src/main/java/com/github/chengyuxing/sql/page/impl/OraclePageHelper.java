package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;
import org.jetbrains.annotations.NotNull;

/**
 * <p>Oracle page helper, e.g.</p>
 * <blockquote>
 * <pre>select *
 * from (select t.*, rownum {@link #ROW_NUM_KEY}
 *       from (...) t
 *       where rownum &lt;= :{@link #END_NUM_KEY})
 *  where {@link #ROW_NUM_KEY} &gt;= :{@link  #START_NUM_KEY}</pre>
 * </blockquote>
 *
 * @see #pagedArgs()
 */
public class OraclePageHelper extends PageHelper {
    public int end() {
        return (int) Math.min((long) pageNumber * pageSize, recordCount);
    }

    public int start() {
        long start = ((long) pageNumber - 1) * pageSize + 1;
        if (start > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Page start exceeds the supported integer range.");
        }
        return (int) start;
    }

    @Override
    public @NotNull String pagedSql(char namedParamPrefix, @NotNull String sql) {
        return "select *\n" +
                "from (select t.*, rownum " + ROW_NUM_KEY + "\n" +
                "          from (\n" + sql + "\n) t\n" +
                "          where rownum <= " + namedParamPrefix + END_NUM_KEY + ")\n" +
                " where " + ROW_NUM_KEY + " >= " + namedParamPrefix + START_NUM_KEY;
    }

    @Override
    public @NotNull Args<Integer> pagedArgs() {
        return Args.of(START_NUM_KEY, start()).add(END_NUM_KEY, end());
    }
}
