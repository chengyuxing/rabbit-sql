package com.github.chengyuxing.sql.annotation;

import java.lang.annotation.*;

/**
 * Mark an interface is a registered xql file mapper.
 * <p>Method behaviors priority:</p>
 * <ol>
 *     <li>Annotated with {@link Insert @Insert} {@link Update @Update} {@link Delete @Delete} {@link Procedure @Procedure}</li>
 *     <li>Annotated with {@link XQL @XQL}(type = {@link Type ...})</li>
 *     <li>Method name pattern:
 *     <ul>
 *         <li>{@link com.github.chengyuxing.sql.XQLInvocationHandler#QUERY_PATTERN select}</li>
 *         <li>{@link com.github.chengyuxing.sql.XQLInvocationHandler#INSERT_PATTERN insert}</li>
 *         <li>{@link com.github.chengyuxing.sql.XQLInvocationHandler#UPDATE_PATTERN update}</li>
 *         <li>{@link com.github.chengyuxing.sql.XQLInvocationHandler#DELETE_PATTERN delete}</li>
 *         <li>{@link com.github.chengyuxing.sql.XQLInvocationHandler#CALL_PATTERN procedure/function}</li>
 *     </ul>
 *     </li>
 * </ol>
 *
 * <p>Method arguments support single argument object or multiple arguments which annotated with {@link Arg @Arg} .</p>
 * <blockquote><pre>
 *     select * from table where id = :id or name = :name
 * </pre></blockquote>
 * Single argument:
 * <blockquote><pre>
 *     // {id: 1, name: "name"}
 *     method(Map&lt;String,Object&gt; args) // Map
 *     method(Entity entity) // Java bean entity
 * </pre></blockquote>
 * Multiple arguments:
 * <blockquote><pre>
 *     method(@Arg("id") int id, @Arg("name") String name)
 * </pre></blockquote>
 * <p>Method return type:</p>
 * <ul>
 *     <li>select: {@link java.util.stream.Stream Stream}
 *     {@link java.util.List List}
 *     {@link java.util.Optional Optional}
 *     {@link java.util.Map Map}
 *     {@link com.github.chengyuxing.common.DataRow DataRow}
 *     {@link com.github.chengyuxing.sql.page.IPageable Ipageable}
 *     {@link com.github.chengyuxing.sql.PagedResource PagedResource}
 *     {@link Integer}
 *     {@link Long}
 *     {@link Double}
 *     {@code <Java Bean>}</li>
 *     <li>insert, update, delete: {@code int} {@link Integer}</li>
 *     <li>procedure, function, ddl, plsql: {@link java.util.Map Map} {@link com.github.chengyuxing.common.DataRow DataRow}</li>
 * </ul>
 *
 * @see XQL @XQL
 * @see CountQuery @CountQuery
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface XQLMapper {
    /**
     * XQL file Alias which defined in {@code xql-file-manager[-*].yml}.
     *
     * @return alias
     */
    String value();
}
