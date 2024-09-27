package com.github.chengyuxing.sql.anno;

import java.lang.annotation.*;

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
