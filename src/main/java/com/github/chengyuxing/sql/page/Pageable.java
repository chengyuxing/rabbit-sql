package com.github.chengyuxing.sql.page;

import java.util.Collections;
import java.util.List;

/**
 * 分页资源
 *
 * @param <T> 类型参数
 */
public final class Pageable<T> {
    private AbstractPageHelper pager;
    private List<T> data;

    private Pageable(AbstractPageHelper pager, List<T> data) {
        this.pager = pager;
        this.data = data;
    }

    public static <T> Pageable<T> of(AbstractPageHelper pager, List<T> data) {
        return new Pageable<>(pager, data);
    }

    public static <T> Pageable<T> empty() {
        return new Pageable<>(null, Collections.emptyList());
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public void setPager(AbstractPageHelper pager) {
        this.pager = pager;
    }

    public List<T> getData() {
        return data;
    }

    public AbstractPageHelper getPager() {
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
