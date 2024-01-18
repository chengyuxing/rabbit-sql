/*
* Created by IntelliJ IDEA.
* User: chengyuxing
* Date: 2024/1/17
* Time: 21:51
* Typing "xql" keyword to get suggestions,
* e.g: "xql:new" will be create a sql fragment.
*/

/*[queryUsers]*/
select *
from test.user
where ${cnd}
order by id desc
;

/*{cnd}*/
-- #if :id != blank
id = :id and name = :name
-- #fi
;