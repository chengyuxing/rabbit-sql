package com.github.chengyuxing.sql.page.impl;

public class Db2PageHelper extends OraclePageHelper {
    @Override
    public String pagedSql(String sql) {
        return "select * from (select rownumber() over() as " + ROW_NUM_KEY + ", t.* from (" + sql + ") as t) where " + ROW_NUM_KEY + " between :" + START_NUM_KEY + " and :" + END_NUM_KEY;
    }
}
