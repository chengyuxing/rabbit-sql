package baki.op;

import com.github.chengyuxing.common.AroundExecutor;
import com.github.chengyuxing.sql.types.Execution;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteCostWatcher extends AroundExecutor<Execution> {
    private static final Logger log = LoggerFactory.getLogger(ExecuteCostWatcher.class);

    @Override
    public void onStart(@NotNull Execution execution) {
        execution.setState("startTime", System.currentTimeMillis());
    }

    @Override
    public void onStop(@NotNull Execution execution, @Nullable Object result, @Nullable Throwable throwable) {
        long startTime = execution.getState("startTime");
        long cost = System.currentTimeMillis() - startTime;
        log.info("{}: {}, SPENT: {} sec.", "SQL Watcher", execution.getSql(), cost / 1000.0);
    }
}
