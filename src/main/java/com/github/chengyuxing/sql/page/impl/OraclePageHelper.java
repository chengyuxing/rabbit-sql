package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

import java.util.Map;

/**
 * <p>Oracle分页工具</p>
 * 默认的分页参数名占位符 {@code :start, :end}<br>
 * e.g.
 * <blockquote>
 * <pre>Select *
 * from (select t.*, rownum rn
 *       from (...) t
 *       where rownum{@code <=} :end)
 *  where rn{@code >=} :start</pre>
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
        return "Select * \n" +
                "from (select t.*,rownum rn \n" +
                "          from (" + sql + ") t\n" +
                "          where rownum <= " + end() + ")\n" +
                " where rn >=" + start();
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
