package com.github.chengyuxing.sql.support;

import com.github.chengyuxing.sql.types.OUTParamType;

/**
 * 函数或存储过程出参类型通用接口，有一个基本的默认的实现，如无法满足，可以自行实现此接口
 *
 * @see OUTParamType
 */
public interface IOutParam {
    /**
     * 获取出参的类型数值
     *
     * @return 类型数值
     * @see java.sql.Types
     */
    int getTypeNumber();

    /**
     * 获取参数类型名
     *
     * @return 类型名
     */
    String getName();
}
