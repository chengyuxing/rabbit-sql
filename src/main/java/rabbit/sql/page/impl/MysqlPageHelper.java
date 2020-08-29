package rabbit.sql.page.impl;

import rabbit.sql.page.PageHelper;

/**
 * Mysql分页工具
 */
public class MysqlPageHelper extends PageHelper {

    public int start() {
        return (getPageNumber() - 1) * getPageSize();
    }

    @Override
    public String wrapPagedSql(String sql) {
        return sql + " limit " + start() + ", " + getPageSize();
    }
}
