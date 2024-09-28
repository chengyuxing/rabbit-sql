package com.github.chengyuxing.sql.annotation;

import java.lang.annotation.*;

/**
 * Method annotated with {@code @Insert} means the method no need to mapping
 * with sql fragment, {@link XQL @XQL} will be not working.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Insert {
    /**
     * Table name.
     *
     * @return table name
     */
    String value();

    /**
     * Ignore null value of data key.
     *
     * @return ignore or not
     */
    boolean ignoreNull() default false;

    /**
     * Ignore data key if table all fields not contains key.
     *
     * @return safe or not
     */
    boolean safe() default false;
}
