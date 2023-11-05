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
 * <blockquote>
 * <ul>
 *     <li>:name (jdbc standard named parameter format, it will parsed to '{@code ?}').</li>
 * </ul>
 * </blockquote>
 * String template variable format:
 * <blockquote>
 * ${...} (will not be prepared)
 *     <ul>
 *         <li>${var}: if boxed basic type array (String[], Integer[]...) or (Set, List...) detected, just expand and replace;</li>
 *         <li>${!var}: starts with '{@code !}', if boxed basic type array (String[], Integer[]...) or (Set, List...) detected, expand and wrap safe single quotes, then replace.</li>
 *     </ul>
 * </blockquote>
 * <p>Notice: in postgresql, some symbol operator such as (?, ?|, ?&amp;, @?) should be write double '{@code ?}' (??, ??|, ??&amp;, @??) to avoid prepare sql error or use function to replace.</p>
 *  e.g.
 * <blockquote>
 * <pre>
 *       select t.id || 'number' || 'name:cyx','{"name": "user"}'::jsonb
 *       from test.user t
 *       where id = :id::integer --suffix type convert
 *       and id {@code >} :idc
 *       and name = text :username --prefix type convert
 *       and '["a","b","c"]'::jsonb{@code ??&} array ['a', 'b'] ${cnd};
 *     </pre>
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
     * @return [prepared sql, sorted arg names, args map，named parameter sql]
     */
    protected Quadruple<String, List<String>, Map<String, Object>, String> prepare(String sql, Map<String, ?> args) {
        // try to generate full named parameter sql.
        Pair<String, Map<String, Object>> result = parseSql(sql, args);
        String parsedSql = result.getItem1();
        Map<String, Object> parsedData = result.getItem2();

        // convert named parameter sql to prepared sql.
        Pair<String, List<String>> p = sqlGenerator().generatePreparedSql(parsedSql, parsedData);
        return Tuples.quadruple(p.getItem1(), p.getItem2(), parsedData, parsedSql);
    }
}
