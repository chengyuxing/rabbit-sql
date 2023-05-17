package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

import java.util.Map;

/**
 * <p>Oracle分页工具</p>
 * 默认的分页参数名占位符 {@code :start, :end}<br>
 * e.g.
 * <blockquote>
 * <pre>SELECT *
 * FROM (SELECT t.*, ROWNUM RN_BY_RABBIT_
 *       FROM (...) t
 *       WHERE ROWNUM{@code <=} :end)
 *  WHERE RN_BY_RABBIT_{@code >=} :start</pre>
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
        return (pageNumber - 1) * pageSize;
    }

    /**
     * 构建分页的SQL，如需自定义分页，请重写此方法
     *
     * @param sql sql 普通未分页的SQL
     * @return 构建分页的SQL
     */
    @Override
    public String pagedSql(String sql) {
        return "SELECT * \n" +
                "FROM (SELECT t.*, ROWNUM RN_4_RABBIT \n" +
                "          FROM (" + sql + ") t\n" +
                "          WHERE ROWNUM <= " + end() + ")\n" +
                " WHERE RN_4_RABBIT >= " + start();
    }

    /**
     * {@inheritDoc} start, end
     *
     * @return 分页参数
     */
    @Override
    public Map<String, Integer> pagedArgs() {
        return Args.of("start", start()).add("end", end());
    }
}
