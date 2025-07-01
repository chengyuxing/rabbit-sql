package com.github.chengyuxing.sql.page.impl;

import org.jetbrains.annotations.NotNull;

public class Db2PageHelper extends OraclePageHelper {
    @Override
    public @NotNull String pagedSql(char namedParamPrefix, @NotNull String sql) {
        return "select * from (select rownumber() over() as " + ROW_NUM_KEY + ", t.* from (\n" + sql + "\n) as t) where " +
                ROW_NUM_KEY + " between " + namedParamPrefix + START_NUM_KEY + " and " + namedParamPrefix + END_NUM_KEY;
    }
}
