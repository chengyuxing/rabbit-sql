package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

import java.util.Map;

/**
 * <p>PostgreSQL分页工具</p>
 * 默认的分页参数名占位符 {@code :limit, :offset}<br>
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

    /**
     * 构建分页的SQL，如需自定义分页，请重写此方法
     *
     * @param sql sql 普通未分页的SQL
     * @return 构建分页的SQL
     */
    @Override
    public String pagedSql(String sql) {
        return sql + " LIMIT " + limit() + " OFFSET " + offset();
    }

    /**
     * {@inheritDoc} limit, offset
     *
     * @return 分页参数
     */
    @Override
    public Map<String, Integer> pagedArgs() {
        return Args.of("limit", limit()).add("offset", offset());
    }
}
