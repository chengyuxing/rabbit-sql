package rabbit.sql.page;

import java.util.Collections;
import java.util.List;

/**
 * 分页资源
 *
 * @param <T> 类型参数
 */
public final class PagedResource<T> {
    private PageHelper pager;
    private List<T> data;

    private PagedResource(PageHelper pager, List<T> data) {
        this.pager = pager;
        this.data = data;
    }

    /**
     * 创建一个分页数据资源对象
     *
     * @param pager 分页对象
     * @param data  数据
     * @param <T>   类型参数
     * @return 分页的数据
     */
    public static <T> PagedResource<T> of(PageHelper pager, List<T> data) {
        return new PagedResource<>(pager, data);
    }

    /**
     * 创建一个空的分页资源对象
     *
     * @param <T> 类型参数
     * @return 空的分页
     */
    public static <T> PagedResource<T> empty() {
        return new PagedResource<>(null, Collections.emptyList());
    }

    void setData(List<T> data) {
        this.data = data;
    }

    void setPager(PageHelper pager) {
        this.pager = pager;
    }

    public List<T> getData() {
        return data;
    }

    public PageHelper getPager() {
        return pager;
    }

    @Override
    public String toString() {
        return "Pageable{" +
                "pager=" + pager +
                ", data=" + data +
                '}';
    }
}
