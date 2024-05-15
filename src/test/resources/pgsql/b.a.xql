/*
* Created by IntelliJ IDEA.
* User: chengyuxing
* Date: 2024/1/17
* Time: 21:51
###
* Typing "xql" keyword to get suggestions,
* e.g: "xql:new" will be create a sql fragment.
###
*/

/*#根据ID查询满足条件的用户#*/
/*[queryUsers]*/
/*#
  Tip：
  1. cnd条件是动态拼接的；
  2. 条件包含参数 id 和 name；
#*/
select *
from test.user
where

    ${cnd}

order by id desc
;

/*{cnd}*/
-- #if :id != blank
id = :id and name = :name
-- #fi
;