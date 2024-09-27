package baki;

import com.github.chengyuxing.sql.anno.Arg;
import com.github.chengyuxing.sql.anno.XQL;
import com.github.chengyuxing.sql.anno.XQLMapper;
import baki.entity.Guest;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.page.IPageable;
import com.github.chengyuxing.sql.types.Param;

import java.util.List;
import java.util.Map;

@XQLMapper("new")
public interface HomeMapper {

    @XQL(type = XQL.Type.FUNCTION)
    DataRow sum(@Arg("a") Param a, @Arg("b") Param b, @Arg("res") Param res);

    @XQL(value = "maven_dependencies_query", type = XQL.Type.PROCEDURE)
    DataRow mavenDependenciesQuery(@Arg("keywords") Param keywords);

    List<DataRow> queryAllGuests();

    PagedResource<Map<String, Object>> queryOneGuest(@Arg("page") int page, @Arg("size") int size);

    @XQL("queryOneGuest")
    IPageable queryGuestsPage(@Arg("page") int page, @Arg("size") int size);

    Guest queryOneGuest();
}
