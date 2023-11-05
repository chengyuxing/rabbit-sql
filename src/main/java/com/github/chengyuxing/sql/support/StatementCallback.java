package com.github.chengyuxing.sql.support;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Prepared statement callback function.
 *
 * @param <T> result type
 */
@FunctionalInterface
public interface StatementCallback<T> {
    /**
     * Do something with prepared statement object.
     *
     * @param statement statement object
     * @return any
     * @throws SQLException if execute sql error
     */
    T doInStatement(PreparedStatement statement) throws SQLException;
}
