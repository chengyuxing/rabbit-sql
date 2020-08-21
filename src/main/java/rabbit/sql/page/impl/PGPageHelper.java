package rabbit.sql.page.impl;

import rabbit.sql.page.PageHelper;

/**
 * PostgreSQL分页工具
 */
public class PGPageHelper extends PageHelper {

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
