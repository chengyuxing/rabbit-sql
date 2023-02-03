package com.github.chengyuxing.sql.page.impl;

public class Db2PageHelper extends OraclePageHelper {
    @Override
    public String pagedSql(String sql) {
        return "select * from (select rownumber() over() as rn ,t.* from (" + sql + ") as t) where rn between " + start() + " and " + end();
    }
}
