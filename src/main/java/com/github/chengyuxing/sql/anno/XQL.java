package com.github.chengyuxing.sql.anno;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface XQL {
    /**
     * Sql fragment name.
     *
     * @return Sql fragment name
     */
    String value() default "";

    /**
     * Sql type.
     *
     * @return Sql type
     */
    Type type() default Type.QUERY;

    enum Type {
        QUERY,
        INSERT,
        UPDATE,
        DELETE,
        PROCEDURE,
        FUNCTION,
        DDL,
        PLSQL
    }
}
