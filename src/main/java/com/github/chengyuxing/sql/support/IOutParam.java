package com.github.chengyuxing.sql.support;

/**
 * Store procedure/function OUT parameter type interface.
 */
@FunctionalInterface
public interface IOutParam {
    /**
     * OUT parameter type number.
     *
     * @return OUT parameter type number
     * @see java.sql.Types
     */
    int typeNumber();
}
