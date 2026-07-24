package com.github.chengyuxing.sql.support;

import java.util.Arrays;

/**
 * Batch execute result object.
 */
public final class BatchResult {
    private final int[] affectedRows;

    public BatchResult(int[] affectedRows) {
        this.affectedRows = affectedRows;
    }

    /**
     * Each row state of the batch result, includes
     * {@link java.sql.Statement#SUCCESS_NO_INFO}
     * {@link java.sql.Statement#EXECUTE_FAILED}
     *
     * @return each row state
     */
    public int[] getAffectedRows() {
        return affectedRows;
    }

    /**
     * Returns the sum of each row state excludes:
     * {@link java.sql.Statement#SUCCESS_NO_INFO}
     * {@link java.sql.Statement#EXECUTE_FAILED}
     *
     * @return sum
     */
    public int getAffectedRowsCount() {
        return Arrays.stream(affectedRows).filter(i -> i >= 0).sum();
    }
}
