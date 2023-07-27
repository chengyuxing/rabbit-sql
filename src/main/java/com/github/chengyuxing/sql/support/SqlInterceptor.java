package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.utils.StringUtil;

import java.sql.DatabaseMetaData;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * sql拦截器
 */
@FunctionalInterface
public interface SqlInterceptor {
    /**
     * 对将要执行的sql进行预处理，true则通过请求并执行，false拒绝执行
     *
     * @param sql      要执行的sql
     * @param args     sql参数
     * @param metaData 当前数据库元数据
     * @return true：执行sql，false：拒绝执行
     * @throws Throwable 抛出异常信息达到拒绝执行
     */
    boolean prevHandle(String sql, Map<String, ?> args, DatabaseMetaData metaData) throws Throwable;

    /**
     * 默认的sql拦截器，拦截DDL语句和无效条件的delete语句
     */
    class DefaultSqlInterceptor implements SqlInterceptor {
        //language=RegExp
        private final Pattern CRITERIA = Pattern.compile("(?<a>[\\w._]+)\\s*=\\s*(?<b>[\\w._]+)");

        @Override
        public boolean prevHandle(String sql, Map<String, ?> args, DatabaseMetaData metaData) throws Throwable {
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
