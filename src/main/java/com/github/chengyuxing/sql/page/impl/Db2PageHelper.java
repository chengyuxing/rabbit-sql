package com.github.chengyuxing.sql.page.impl;

public class Db2PageHelper extends OraclePageHelper {
    @Override
    public String pagedSql(String sql) {
        return "select * from (select rownumber() over() as rn_4_rabbit, t.* from (" + sql + ") as t) where rn_4_rabbit between " + start() + " and " + end();
    }
}
