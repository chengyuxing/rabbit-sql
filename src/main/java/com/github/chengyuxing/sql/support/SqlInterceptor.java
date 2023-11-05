package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.exceptions.IllegalSqlException;

import java.sql.DatabaseMetaData;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sql interceptor.
 */
@FunctionalInterface
public interface SqlInterceptor {
    /**
     * Pre handle before sql real execute.
     *
     * @param sql      sql
     * @param args     sql parameter data
     * @param metaData current database metadata
     * @return true if valid or false
     * @throws IllegalSqlException reject execute exception
     */
    boolean preHandle(String sql, Map<String, ?> args, DatabaseMetaData metaData) throws IllegalSqlException;

    /**
     * Default sql interceptor (reject drop/create/alter and delete without condition).
     */
    class DefaultSqlInterceptor implements SqlInterceptor {
        //language=RegExp
        private final Pattern CRITERIA = Pattern.compile("(?<a>[\\w._]+)\\s*=\\s*(?<b>[\\w._]+)");

        @Override
        public boolean preHandle(String sql, Map<String, ?> args, DatabaseMetaData metaData) {
            if (StringUtil.isEmpty(sql)) {
                return false;
            }
            String mySql = sql.trim();
            if (StringUtil.startsWithsIgnoreCase(mySql, "drop", "create", "alter")) {
                return false;
            }
            if (StringUtil.startsWithIgnoreCase(mySql, "delete")) {
                if (!StringUtil.containsIgnoreCase(mySql, "where")) {
                    return false;
                }
                Matcher m = CRITERIA.matcher(mySql);
                if (m.find()) {
                    return !m.group("a").equals(m.group("b"));
                }
            }
            return true;
        }
    }
}
