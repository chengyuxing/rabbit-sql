package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.tuple.Quadruple;
import com.github.chengyuxing.common.tuple.Tuples;
import com.github.chengyuxing.sql.utils.SqlGenerator;

import java.util.List;
import java.util.Map;

/**
 * <h2>Support parse named parameter sql</h2>
 * <p>Named parameter prefix symbol depends on implementation of {@link #sqlGenerator()}.</p>
 * Named parameter format e.g if named parameter prefix is '{@code :}' :
 * <ul>
 *     <li>{@code :name} (jdbc standard named parameter format, it will parsed to '{@code ?}').</li>
 * </ul>
 * <p>String template ({@code ${...}} will not be prepared) variable format:</p>
 * <ul>
 *   <li>{@code ${var}}: if boxed basic type array ({@link  String String[]}, {@link Integer Integer[]}, ...) or ({@link java.util.Set Set}, {@link List List}, ...) detected, just expand and replace;</li>
 *   <li>{@code ${!var}}: starts with '{@code !}', if boxed basic type array ({@link  String String[]}, {@link Integer Integer[]}, ...) or ({@link java.util.Set Set}, {@link List List}, ...) detected, expand and wrap safe single quotes, then replace.</li>
 * </ul>
 * <p>Notice: in postgresql, some symbol operator such as ({@code ?}, {@code ?|}, {@code ?&}, {@code @?}) should be write double '{@code ?}' ({@code ??}, {@code ??|}, {@code ??&}, {@code @??}) to avoid prepare sql error or use function to replace.</p>
 * <p>e.g. postgresql sql statement:</p>
 * <blockquote>
 * <pre>
 * select t.id || 'number' || 'name:cyx','{"name": "user"}'::jsonb
 * from test.user t
 * where id = :id::integer
 * and id &gt; :idc
 * and name = text :username
 * and '["a","b","c"]'::jsonb ??&amp; array ['a', 'b']
 * ${cnd}</pre>
 * </blockquote>
 *
 * @see SqlGenerator
 */
public abstract class SqlParser {

    /**
     * Do some prepare work for parse source sql.
     *
     * @param sql  sql
     * @param args args
     * @return parsed sql and args
     */
    protected abstract Pair<String, Map<String, Object>> parseSql(String sql, Map<String, ?> args);

    /**
     * Sql translator for support prepare sql.
     *
     * @return Sql translator.
     */
    protected abstract SqlGenerator sqlGenerator();

    /**
     * Convert named parameter sql to prepared sql.
     *
     * @param sql  named parameter sql
     * @param args args
     * @return [prepared sql, ordered arg names, args map，named parameter sql]
     */
    protected Quadruple<String, Map<String, List<Integer>>, Map<String, Object>, String> prepare(String sql, Map<String, ?> args) {
        // try to generate full named parameter sql.
        Pair<String, Map<String, Object>> result = parseSql(sql, args);
        String parsedSql = result.getItem1();
        Map<String, Object> parsedData = result.getItem2();

        // convert named parameter sql to prepared sql.
        Pair<String, Map<String, List<Integer>>> p = sqlGenerator().generatePreparedSql(parsedSql, parsedData);
        return Tuples.of(p.getItem1(), p.getItem2(), parsedData, parsedSql);
    }
}
