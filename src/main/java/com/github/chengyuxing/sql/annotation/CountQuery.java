package com.github.chengyuxing.sql.annotation;

import com.github.chengyuxing.sql.page.IPageable;

import java.lang.annotation.*;

/**
 * Only work with method which return type is {@link IPageable IPageable}
 * or {@link com.github.chengyuxing.sql.PagedResource PagedResource}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface CountQuery {
    /**
     * Count query name.
     *
     * @return query name
     */
    String value();
}
