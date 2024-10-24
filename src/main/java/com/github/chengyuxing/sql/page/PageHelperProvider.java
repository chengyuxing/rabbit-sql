package com.github.chengyuxing.sql.page;

import com.github.chengyuxing.sql.page.impl.Db2PageHelper;
import com.github.chengyuxing.sql.page.impl.MysqlPageHelper;
import com.github.chengyuxing.sql.page.impl.OraclePageHelper;
import com.github.chengyuxing.sql.page.impl.PGPageHelper;

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
     * @see PGPageHelper PGPageHelper
     * @see OraclePageHelper OraclePageHelper
     * @see MysqlPageHelper MysqlPageHelper
     * @see Db2PageHelper Db2PageHelper
     */
    PageHelper customPageHelper(DatabaseMetaData databaseMetaData, String dbName, char namedParamPrefix);
}
