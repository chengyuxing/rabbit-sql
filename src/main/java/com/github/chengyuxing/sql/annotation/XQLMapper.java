package com.github.chengyuxing.sql.annotation;

import java.lang.annotation.*;

/**
 * Mark an Interface is a registered xql file mapper.
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
 *
 * @see XQL
 * @see Insert
 * @see Delete
 * @see Update
 * @see Procedure
 * @see CountQuery
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
