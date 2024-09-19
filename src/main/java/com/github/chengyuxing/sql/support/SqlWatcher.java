package com.github.chengyuxing.sql.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sql watcher.
 */
@FunctionalInterface
public interface SqlWatcher {
    /**
     * Watch sql execution information.
     *
     * @param sql       sql
     * @param args      args
     * @param startTime connection request time
     * @param endTime   execute finish time
     */
    void watch(String sql, Object args, long startTime, long endTime);

    class SqlWatchLogger implements SqlWatcher {
        private static final Logger log = LoggerFactory.getLogger(SqlWatchLogger.class);
        private static final String PREFIX = "SQL-WATCHER";

        @Override
        public void watch(String sql, Object args, long startTime, long endTime) {
            double spent = (endTime - startTime) / 1000.0;
            log.info("{}: {}, SPENT: {} sec.", PREFIX, sql, spent);
        }
    }
}
