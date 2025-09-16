package baki.op;

import com.github.chengyuxing.sql.plugins.ExecutionWatcher;
import com.github.chengyuxing.sql.types.Execution;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecuteCostWatcher implements ExecutionWatcher {
    private static final Logger log = LoggerFactory.getLogger(ExecuteCostWatcher.class);

    @Override
    public void onStart(Execution execution) {
        execution.setState("startTime", System.currentTimeMillis());
    }

    @Override
    public void onStop(Execution execution, @Nullable Object result, @Nullable Throwable throwable) {
        long startTime = execution.getState("startTime");
        long cost = System.currentTimeMillis() - startTime;
        log.info("{}: {}, SPENT: {} sec.", "SQL Watcher", execution.getSql(), cost / 1000.0);
    }
}
