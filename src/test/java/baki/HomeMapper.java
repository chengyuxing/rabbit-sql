package baki;

import com.github.chengyuxing.sql.annotation.*;
import baki.entity.Guest;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.PagedResource;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.annotation.Type;

import java.util.List;
import java.util.Map;

@XQLMapper("new")
public interface HomeMapper {

    default int now() {
        return 1;
    }

    @XQL(type = Type.function)
    DataRow sum(@Arg("a") Param a, @Arg("b") Param b, @Arg("res") Param res);

    @XQL(type = Type.procedure)
    DataRow mavenDependenciesQuery(@Arg("keywords") Param keywords);

    List<DataRow> queryAllGuests();

    @CountQuery("queryAllGuestsCount")
    @PageableConfig(disableDefaultPageSql = {"limit", "offset"})
    PagedResource<DataRow> queryAllGuests(@Arg("page") int page, @Arg("size") int size);

    @CountQuery("queryGuestsCount")
    PagedResource<Map<String, Object>> queryOneGuest(@Arg("page") int page, @Arg("size") int size);

    Guest queryOneGuest();

}
