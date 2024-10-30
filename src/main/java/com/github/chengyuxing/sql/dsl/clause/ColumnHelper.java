package com.github.chengyuxing.sql.dsl.clause;

import com.github.chengyuxing.sql.dsl.type.ColumnReference;
import com.github.chengyuxing.sql.dsl.type.Operator;
import com.github.chengyuxing.sql.dsl.type.StandardOperator;
import com.github.chengyuxing.sql.utils.EntityUtil;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.Set;

public abstract class ColumnHelper<T> {
    protected final Class<T> clazz;

    protected ColumnHelper(@NotNull Class<T> clazz) {
        this.clazz = clazz;
    }

    protected abstract Set<String> columnWhiteList();

    protected abstract Set<String> operatorWhiteList();

    protected @NotNull String getColumnName(@NotNull ColumnReference<T> columnReference) {
        String fieldName = EntityUtil.getFieldNameWithCache(columnReference);
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return EntityUtil.getColumnName(field);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean isIllegalColumn(String column) {
        if (Objects.isNull(columnWhiteList()) || columnWhiteList().isEmpty()) {
            return true;
        }
        return !columnWhiteList().contains(column);
    }

    protected boolean isIllegalOperator(@NotNull Operator operator) {
        if (operator instanceof StandardOperator) {
            return false;
        }
        if (Objects.isNull(operatorWhiteList()) || operatorWhiteList().isEmpty()) {
            return true;
        }
        return !operatorWhiteList().contains(operator.getValue());
    }
}
