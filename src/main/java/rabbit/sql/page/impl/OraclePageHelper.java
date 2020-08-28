package rabbit.sql.page.impl;

import rabbit.sql.page.PageHelper;

/**
 * oracle分页工具
 */
public class OraclePageHelper extends PageHelper {
    public int end() {
        int end = getPageNumber() * getPageSize();
        if (end > getRecordCount())
            end = getRecordCount();
        return end;
    }

    public int start() {
        return (getPageNumber() - 1) * getPageSize();
    }

    @Override
    public String wrapPagedSql(String sql) {
        return "Select * \n" +
                "from (select t.*,rownum rn \n" +
                "          from (" + sql + ") t\n" +
                "          where rownum <= " + end() + ")\n" +
                " where rn >=" + start();
    }
}
