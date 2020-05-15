package org.rabbit.sql.page.impl;

import org.rabbit.sql.page.AbstractPageHelper;

/**
 * PostgreSQL分页工具
 */
public class PGPageHelper extends AbstractPageHelper {

    private PGPageHelper(int page, int size) {
        super(page, size);
    }

    public static PGPageHelper of(int page, int size) {
        return new PGPageHelper(page, size);
    }

    public int limit() {
        return getPageSize();
    }

    public int offset() {
        return (getPageNumber() - 1) * getPageSize();
    }

    @Override
    public String wrapPagedSql(String sql) {
        return sql + " LIMIT " + limit() + " OFFSET " + offset();
    }
}
