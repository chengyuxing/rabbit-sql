package com.github.chengyuxing.sql.support;

/**
 * 函数或存储过程出参类型通用接口
 */
@FunctionalInterface
public interface IOutParam {
    /**
     * 获取出参的类型数值
     *
     * @return 类型数值
     * @see java.sql.Types
     */
    int typeNumber();
}
