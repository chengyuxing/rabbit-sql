package com.github.chengyuxing.sql.page.impl;

import org.jetbrains.annotations.NotNull;

/**
 * <p>SqlServer 2012+ page helper, e.g.</p>
 * <blockquote>
 * <pre>select * from ...order by ...
 * offset :{@link  #END_NUM_KEY} rows fetch next :{@link #START_NUM_KEY} rows only;</pre>
 * </blockquote>
 *
 * @see #pagedArgs()
 */
public class SqlServer2012PageHelper extends PGPageHelper {
    @Override
    public @NotNull String pagedSql(char namedParamPrefix, @NotNull String sql) {
        return sql + " offset " + namedParamPrefix + END_NUM_KEY + " rows fetch next " + namedParamPrefix + START_NUM_KEY + " rows only";
    }
}
