/**
* -------XQL文件内容格式说明-------
* 文件格式为标准的sql文件格式进行了一点扩展;
* 具体格式和规范可下载源码查看（XQLFileManager）注释或文档：
* https://github.com/chengyuxing/rabbit-sql#Prepare-SQL
* https://github.com/chengyuxing/rabbit-sql#xqlfilemanager
**/

/*[query]*/
select *
from test."user" t ${part1};

/*第一部分*/
/*{part1}*/
where id = :id ${order};

/*{order}*/
order by id;

/*[for]*/
select * from test.user where id = 1
-- #for id of :ids delimiter ', ' open ' or id in (' close ')'
    -- #if :id > 0
    :_for.id
    -- #fi
-- #done
;