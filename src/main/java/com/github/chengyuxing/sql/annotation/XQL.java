package com.github.chengyuxing.sql.annotation;

import java.lang.annotation.*;

/**
 * Specify the method if method name not equals to sql fragment name
 * or method behaviour is not do query.
 */
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
    Type type() default Type.query;
}
