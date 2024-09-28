package com.github.chengyuxing.sql.annotation;

import java.lang.annotation.*;

/**
 * Specify the method arg name mapping to sql named parameter, e,g sql
 * <blockquote><pre>select * from table where id = :id
 * </pre></blockquote>
 * Method:
 * <blockquote><pre>method(@Arg("id") int id)
 * </pre></blockquote>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
@Documented
public @interface Arg {
    /**
     * Arg name.
     *
     * @return arg name
     */
    String value();
}
