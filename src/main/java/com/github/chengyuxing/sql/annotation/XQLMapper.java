package com.github.chengyuxing.sql.annotation;

import com.github.chengyuxing.sql.page.IPageable;

import java.lang.annotation.*;

/**
 * Mark an interface as a registered xql file mapper.
 * <p>Method behaviors priority:</p>
 * <ol>
 *     <li>Annotated with
 *     {@link Procedure @Procedure}
 *     {@link Function @Function}
 *     </li>
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
 *     {@link IPageable IPageable}
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
 * @see PageableConfig @PageableConfig
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
