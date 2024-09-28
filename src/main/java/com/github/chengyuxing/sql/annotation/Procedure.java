package com.github.chengyuxing.sql.annotation;

import java.lang.annotation.*;

/**
 * Method annotated with {@code @Procedure} means the method no need to mapping
 * with sql fragment, {@link XQL @XQL} will be not working.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Procedure {
    /**
     * Procedure or function name.
     *
     * @return procedure or function name
     */
    String value();
}
