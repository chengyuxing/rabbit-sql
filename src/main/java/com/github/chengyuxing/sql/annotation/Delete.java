package com.github.chengyuxing.sql.annotation;

import java.lang.annotation.*;

/**
 * Method annotated with {@code @Delete} means the method no need to mapping
 * with sql fragment, {@link XQL @XQL} will be not working.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Delete {
    /**
     * Table name.
     */
    String value();

    /**
     * Where condition.
     */
    String where();
}
