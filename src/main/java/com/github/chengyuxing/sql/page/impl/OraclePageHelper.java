package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

/**
 * <p>Oracle page helper, e.g.</p>
 * <blockquote>
 * <pre>SELECT *
 * FROM (SELECT t.*, ROWNUM RN_4_RABBIT
 *       FROM (...) t
 *       WHERE ROWNUM &lt;= :{@link #END_NUM_KEY})
 *  WHERE RN_4_RABBIT &gt;= :{@link  #START_NUM_KEY}</pre>
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
    public String pagedSql(String sql) {
        return "SELECT * \n" +
                "FROM (SELECT t.*, ROWNUM " + ROW_NUM_KEY + " \n" +
                "          FROM (" + sql + ") t\n" +
                "          WHERE ROWNUM <= " + end() + ")\n" +
                " WHERE " + ROW_NUM_KEY + " >= " + start();
    }

    @Override
    public Args<Integer> pagedArgs() {
        return Args.of(START_NUM_KEY, start()).add(END_NUM_KEY, end());
    }
}
