package rabbit.sql.page.impl;

import rabbit.sql.page.PageHelper;

/**
 * oracle分页工具
 */
public class OraclePageHelper extends PageHelper {
    private OraclePageHelper(int page, int size) {
        super(page, size);
    }

    public static OraclePageHelper of(int page, int size) {
        return new OraclePageHelper(page, size);
    }

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
