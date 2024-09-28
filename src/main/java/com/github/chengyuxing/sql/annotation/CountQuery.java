package com.github.chengyuxing.sql.annotation;

import java.lang.annotation.*;

/**
 * Only work with method which return type is {@link com.github.chengyuxing.sql.page.IPageable IPageable}
 * or {@link com.github.chengyuxing.sql.PagedResource PagedResource}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface CountQuery {
    /**
     * Count query name.
     */
    String value();
}
