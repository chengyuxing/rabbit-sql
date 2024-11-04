package com.github.chengyuxing.sql.annotation;

import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.page.PageHelper;
import com.github.chengyuxing.sql.plugins.PageHelperProvider;

import java.lang.annotation.*;
import java.util.function.Function;

/**
 * IPageable configuration.<br>
 * Only work with method which return type is {@link IPageable IPageable}
 * or {@link com.github.chengyuxing.sql.PagedResource PagedResource}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface PageableConfig {
    /**
     * Disable auto generate paged sql ({@link PageHelper#pagedSql(char, String)} and rewrite default page args.
     *
     * @return default page args [{@link PageHelper#START_NUM_KEY start}, {@link PageHelper#END_NUM_KEY end}]
     * @see IPageable#rewriteDefaultPageArgs(Function) rewriteDefaultPageArgs
     */
    String[] disableDefaultPageSql() default {};

    /**
     * Set custom page helper provider for current page query.
     *
     * @return page help provider class
     */
    Class<? extends PageHelperProvider> pageHelper() default PageHelperProvider.class;
}
