package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.sql.page.PageHelper;

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
     */
    PageHelper customPageHelper(DatabaseMetaData databaseMetaData, String dbName, char namedParamPrefix);
}
