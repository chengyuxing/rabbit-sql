package com.github.chengyuxing.sql.annotation;

import java.lang.annotation.*;

/**
 * Method annotated with {@code @Update} means the method no need to mapping
 * with sql fragment, {@link XQL @XQL} will be not working.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Update {
    /**
     * Table name.
     */
    String value();

    /**
     * Where condition.
     */
    String where();

    /**
     * Ignore null value of data key.
     */
    boolean ignoreNull() default false;

    /**
     * Ignore data key if table all fields not contains key.
     */
    boolean safe() default false;
}
