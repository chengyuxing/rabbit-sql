package rabbit.sql.page.impl;

import rabbit.sql.page.PageHelper;

/**
 * PostgreSQL分页工具
 */
public class PGPageHelper extends PageHelper {

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
