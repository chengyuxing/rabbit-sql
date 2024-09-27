package com.github.chengyuxing.sql.anno;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface XQLMapper {
    /**
     * XQL file Alias.
     *
     * @return alias
     */
    String value();
}
