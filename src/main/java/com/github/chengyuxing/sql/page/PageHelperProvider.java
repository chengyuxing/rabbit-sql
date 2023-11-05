package com.github.chengyuxing.sql.page;

import java.sql.DatabaseMetaData;

/**
 * Page helper provider
 */
@FunctionalInterface
public interface PageHelperProvider {
    /**
     * Implement custom page helper.
     *
     * @param databaseMetaData current database metadata
     * @param dbName           current database name
     * @param namedParamPrefix current named parameter prefix
     * @return PageHelper instance
     * @see com.github.chengyuxing.sql.page.impl.PGPageHelper PGPageHelper
     * @see com.github.chengyuxing.sql.page.impl.OraclePageHelper OraclePageHelper
     * @see com.github.chengyuxing.sql.page.impl.MysqlPageHelper MysqlPageHelper
     * @see com.github.chengyuxing.sql.page.impl.Db2PageHelper Db2PageHelper
     */
    PageHelper customPageHelper(DatabaseMetaData databaseMetaData, String dbName, char namedParamPrefix);
}
