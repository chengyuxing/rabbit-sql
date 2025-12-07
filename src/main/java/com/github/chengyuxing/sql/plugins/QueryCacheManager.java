package com.github.chengyuxing.sql.plugins;

import com.github.chengyuxing.common.DataRow;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Query cache manager.
 */
public interface QueryCacheManager {

    @FunctionalInterface
    interface RawQueryProvider {
        @NotNull Stream<DataRow> apply(@NotNull String sql, Map<String, ?> args);
    }

    /**
     * Get cache.
     *
     * @param sql      sql name or sql string
     * @param args     the sql args
     * @param provider the non-cached database query interface for supports the cache source.
     * @return cache or null
     */
    @NotNull Stream<DataRow> get(@NotNull String sql, Map<String, ?> args, @NotNull RawQueryProvider provider);

    /**
     * Check the sql matches the conditions.
     *
     * @param sql  sql name or sql string
     * @param args args
     * @return true or false
     */
    boolean isAvailable(@NotNull String sql, Map<String, ?> args);

}
