package com.github.chengyuxing.sql.annotation;

import org.intellij.lang.annotations.Language;

import java.lang.annotation.*;

/**
 * <p>Procedure name e.g. {@code {call my_proc(:num)}} .</p>
 * Method annotated with {@code @Procedure} means the method no need to mapping
 * with sql fragment, {@link XQL @XQL} will be not working.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Procedure {
    /**
     * Procedure name.
     *
     * @return procedure name
     */
    @Language("SQL")
    String value();
}
