package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

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
        int end = pageNumber * pageSize;
        if (end > recordCount)
            end = recordCount;
        return end;
    }

    public int start() {
        return (pageNumber - 1) * pageSize + 1;
    }

    @Override
    public String pagedSql(char namedParamPrefix, String sql) {
        return "select *\n" +
                "from (select t.*, rownum " + ROW_NUM_KEY + "\n" +
                "          from (" + sql + ") t\n" +
                "          where rownum <= " + namedParamPrefix + END_NUM_KEY + ")\n" +
                " where " + ROW_NUM_KEY + " >= " + namedParamPrefix + START_NUM_KEY;
    }

    @Override
    public Args<Integer> pagedArgs() {
        return Args.of(START_NUM_KEY, start()).add(END_NUM_KEY, end());
    }
}
