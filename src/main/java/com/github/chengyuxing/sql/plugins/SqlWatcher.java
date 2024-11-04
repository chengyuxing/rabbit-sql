package com.github.chengyuxing.sql.plugins;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
     * @param sourceSql source sql
     * @param targetSql target sql
     * @param args      args
     * @param startTime connection request time
     * @param endTime   execute finish time
     * @param throwable throwable
     */
    void watch(@NotNull String sourceSql, @NotNull String targetSql, @Nullable Object args, long startTime, long endTime, @Nullable Throwable throwable);

    class SqlWatchLogger implements SqlWatcher {
        private static final Logger log = LoggerFactory.getLogger(SqlWatchLogger.class);
        private static final String PREFIX = "SQL-WATCHER";

        @Override
        public void watch(@NotNull String sourceSql, @NotNull String targetSql, Object args, long startTime, long endTime, @Nullable Throwable throwable) {
            double spent = (endTime - startTime) / 1000.0;
            log.info("{}: {}, SPENT: {} sec.", PREFIX, sourceSql, spent);
        }
    }
}
