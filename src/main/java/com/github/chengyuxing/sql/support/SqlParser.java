package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.tuple.Triple;
import com.github.chengyuxing.sql.utils.SqlGenerator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <h2>提供对命名参数sql的预解析处理</h2>
 * <p>默认的命名参数前缀为 ':' 号，可通过实现 {@link #sqlGenerator()} 来进行自定义。</p>
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
 * @see SqlGenerator
 */
public abstract class SqlParser {

    /**
     * 提供一个抽象方法供实现类对单前要执行的sql做一些准备操作
     *
     * @param sql  sql
     * @param args 参数
     * @return 处理后的sql和参数
     */
    protected abstract Pair<String, Map<String, Object>> parseSql(String sql, Map<String, ?> args);

    /**
     * sql翻译帮助
     *
     * @return sql翻译帮助实例
     */
    protected abstract SqlGenerator sqlGenerator();

    /**
     * 将自定义的传名参数sql解析为数据库可执行的预编译sql
     *
     * @param sql  传名参数sql
     * @param args 参数字典
     * @return 预编译sql和参数名和参数字典
     */
    protected Triple<String, List<String>, Map<String, Object>> prepare(String sql, Map<String, ?> args) {
        Map<String, ?> data = args == null ? Collections.emptyMap() : args;
        Pair<String, Map<String, Object>> result = parseSql(sql, data);
        String parsedSql = result.getItem1();
        Map<String, Object> parsedData = result.getItem2();
        Pair<String, List<String>> p = sqlGenerator().generatePreparedSql(parsedSql, parsedData);
        return Triple.of(p.getItem1(), p.getItem2(), parsedData);
    }
}
