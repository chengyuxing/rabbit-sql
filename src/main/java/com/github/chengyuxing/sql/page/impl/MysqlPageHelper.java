package com.github.chengyuxing.sql.page.impl;

import com.github.chengyuxing.sql.Args;
import com.github.chengyuxing.sql.page.PageHelper;

/**
 * <p>Mysql/MariaDB分页工具</p>
 * 默认的分页参数名占位符 {@code :limit, :size}<br>
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

    /**
     * 构建分页的SQL，如需自定义分页，请重写此方法
     *
     * @param sql sql 普通未分页的SQL
     * @return 构建分页的SQL
     */
    @Override
    public String pagedSql(String sql) {
        return sql + " limit " + start() + ", " + size();
    }

    /**
     * {@inheritDoc} limit, size
     *
     * @return 分页参数
     */
    @Override
    public Args<Integer> pagedArgs() {
        return Args.of("limit", start()).add("size", size());
    }
}
