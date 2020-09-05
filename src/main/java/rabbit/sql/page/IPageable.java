package rabbit.sql.page;

import rabbit.common.types.DataRow;

import java.util.Map;
import java.util.function.Function;

/**
 * 分页资源接口
 *
 * @param <T> 结果类型参数
 */
public interface IPageable<T> {
    /**
     * 参数
     *
     * @param args 参数
     * @return 分页构建器
     */
    IPageable<T> args(Map<String, Object> args);

    /**
     * 记录条数查询sql
     *
     * @param countQuery 记录条数查询sql
     * @return 分页构建器
     */
    IPageable<T> countQuery(String countQuery);

    /**
     * 设置分页帮助类
     *
     * @param pageHelper 分页帮助类
     * @return 分页资源对象
     */
    IPageable<T> pageHelper(PageHelper pageHelper);

    /**
     * 收集已分页的资源
     *
     * @param valueConvert 结果类型转换
     * @return 分页对象和数据
     */
    PagedResource<T> collect(Function<DataRow, T> valueConvert);
}
