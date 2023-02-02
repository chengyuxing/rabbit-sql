package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.sql.utils.SqlTranslator;
import com.github.chengyuxing.sql.utils.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <h2>提供对命名参数sql的预解析处理</h2>
 * <p>默认的命名参数前缀为 ':' 号，可通过实现 {@link #sqlTranslator()} 来进行自定义。</p>
 * 命名参数格式：
 * <blockquote>
 * :name (jdbc标准的传名参数写法，参数将被预编译安全处理)
 * </blockquote>
 * <br>
 * 字符串模版参数名格式：
 * <blockquote>
 * ${...} (通用的字符串模版占位符，不进行预编译，用于动态sql的拼接)
 *     <ul>
 *         <li>${part} 如果类型是装箱类型数组(String[], Integer[]...)或集合(Set, List...)，则先展开（逗号分割），再进行sql片段的替换；</li>
 *         <li>${:part} 名字前多了前缀符号(:)，如果类型是装箱类型数组(String[], Integer[]...)或集合(Set, List...)，则先展开（逗号分隔），并做一定的字符串安全处理，再进行sql片段的替换。</li>
 *     </ul>
 * </blockquote>
 * <p>小提示：PostgreSQL中，带有问号的操作符(?,?|,?&amp;,@?)可以使用双问号(??,??|,??&amp;,@??)解决预编译sql参数未设定的报错，或者直接使用函数</p>
 * 可以被正确处理的SQL字符串例如：
 * <blockquote>
 * <pre>
 *       select t.id || 'number' || 'name:cyx','{"name": "user"}'::jsonb
 *       from test.user t
 *       where id = :id::integer --后缀类型转换
 *       and id {@code >} :idc
 *       and name = text :username --前缀类型转换
 *       and '["a","b","c"]'::jsonb{@code ??&} array ['a', 'b'] ${cnd};
 *     </pre>
 * </blockquote>
 *
 * @see SqlTranslator
 */
public abstract class SqlSupport {
    private static final Logger log = LoggerFactory.getLogger(SqlSupport.class);

    /**
     * 提供一个抽象方法供实现类对单前要执行的sql做一些准备操作
     *
     * @param sql  sql
     * @param args 参数
     * @return 处理后的sql
     */
    protected abstract String getSql(String sql, Map<String, ?> args);

    /**
     * sql翻译帮助
     *
     * @return sql翻译帮助实例
     */
    protected abstract SqlTranslator sqlTranslator();

    /**
     * debug模式输出完整的sql，否则仅输出原始sql
     *
     * @return 是否输出已执行sql
     */
    protected abstract boolean debugFullSql();

    /**
     * debug模式下终端标准输出sql语法是否高亮
     *
     * @return 是否高亮
     */
    protected abstract boolean highlightSql();

    /**
     * 将自定义的传名参数sql解析为数据库可执行的预编译sql
     *
     * @param sql  传名参数sql
     * @param args 参数字典
     * @return 预编译sql和参数名
     */
    protected Pair<String, List<String>> compileSql(String sql, Map<String, ?> args) {
        if (args == null) {
            args = Collections.emptyMap();
        }
        String sourceSql = getSql(sql, args);
        Pair<String, List<String>> preparedSqlAndArgNames = sqlTranslator().getPreparedSql(sourceSql, args);
        if (log.isDebugEnabled()) {
            log.debug("SQL:{}", SqlUtil.buildPrintSql(sourceSql, highlightSql()));
            log.debug("Args:{}", args);
            if (debugFullSql()) {
                String fullSql = sqlTranslator().generateSql(sourceSql, args, false).getItem1();
                log.debug("Full SQL: {}", SqlUtil.buildPrintSql(fullSql, highlightSql()));
            }
        }
        return preparedSqlAndArgNames;
    }
}
