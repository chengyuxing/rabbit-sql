package com.github.chengyuxing.sql.support;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 预编译sql语句参数值处理器
 */
@FunctionalInterface
public interface StatementValueHandler {
    /**
     * 自定义处理预编译sql参数值。<br>
     * 可通过提供的{@link DatabaseMetaData} 获取一些有用的信息来针对性的处理特殊的参数值，例如：
     * <ul>
     *     <li>{@link DatabaseMetaData#getDatabaseProductName() getDatabaseProductName()} 获取当前数据库的名字</li>
     *     <li>{@link DatabaseMetaData#getDatabaseProductVersion() getDatabaseProductVersion()} 获取当前数据库的版本</li>
     * </ul>
     *
     * @param ps       预编译对象（{@link PreparedStatement} | {@link java.sql.CallableStatement CallableStatement}）
     * @param index    参数序号
     * @param value    参数值
     * @param metaData 当前数据库元数据（默认连接为关闭状态，可获取一些离线属性，部分连接池的实现并未完全关闭，注意连接泄漏的风险）
     * @see com.github.chengyuxing.sql.utils.JdbcUtil#setStatementValue(PreparedStatement, int, Object) JdbcUtil.setStatementValue(PreparedStatement, int, Object)
     */
    void preHandle(PreparedStatement ps, int index, Object value, DatabaseMetaData metaData) throws SQLException;
}
