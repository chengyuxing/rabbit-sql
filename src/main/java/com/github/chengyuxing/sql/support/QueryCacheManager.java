package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.common.DataRow;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Query cache manager.
 */
public interface QueryCacheManager {
    /**
     * Get cache.
     * <p>Notice: Do not return an empty stream if the cache does not exist.
     * To trigger the execution of a database query, null must be returned.</p>
     *
     * @param sql    sql name
     * @param params params
     * @return cache or null
     */
    Stream<DataRow> get(String sql, Map<String, Object> params);

    /**
     * Put cache.
     *
     * @param sql    sql name
     * @param params params
     * @param value  query result
     */
    void put(String sql, Map<String, Object> params, List<DataRow> value);

    /**
     * Invalidate cache.
     *
     * @param sql    sql name
     * @param params params
     */
    default void invalidate(String sql, Map<String, Object> params) {
    }

    /**
     * Check the sql matches the conditions or not, if matches,
     * {@link #put(String, Map, List) put} and {@link #get(String, Map) get} will be enabling.
     *
     * @param sql    sql name
     * @param params params
     * @return true or false
     */
    boolean isAvailable(String sql, Map<String, Object> params);

}
