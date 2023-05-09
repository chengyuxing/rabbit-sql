package com.github.chengyuxing.sql.page;

import java.sql.DatabaseMetaData;

/**
 * 分页帮助提供程序，用于实现自定义的分页
 */
@FunctionalInterface
public interface PageHelperProvider {
    /**
     * 自定义分页帮助实现
     *
     * @param databaseMetaData 当前数据库元数据
     * @param dbName           当前数据库名称，不区分大小写
     * @param namedParamPrefix 当前sql命名参数前缀符号，最终将被预编译
     * @return 分页帮助实现实例
     * @see com.github.chengyuxing.sql.page.impl.PGPageHelper PGPageHelper
     * @see com.github.chengyuxing.sql.page.impl.OraclePageHelper OraclePageHelper
     * @see com.github.chengyuxing.sql.page.impl.MysqlPageHelper MysqlPageHelper
     */
    PageHelper customPageHelper(DatabaseMetaData databaseMetaData, String dbName, char namedParamPrefix);
}
