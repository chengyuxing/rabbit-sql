/*
* Created by IntelliJ IDEA.
* User: chengyuxing
* Date: 2024/1/17
* Time: 21:51
@@@
curl -T /Volumes/TU280Pro/work/zhag-server-2.94.tar.gz
ftp://ftpuser:skynet123456@221.3.220.93:6021
@@@
* Typing "xql" keyword to get suggestions,
* e.g: "xql:new" will be create a sql fragment.
*/

/*#根据ID查询满

  足条件的用户#*/
/*aaaaaaaadd
  ddddddd*/
/*[queryUsers]*/
    /*ccccccccccc*/
/*#
  Tip：
  1. cnd条件是动态拼接的；
  2. 条件包含参数 id 和 name；
#*/
/*ddddddjajjjaa*/
select *
from test.user
-- inner join test.tb on 1 = 1
where

    ${cnd}

order by id desc
;

/*{cnd}*/
-- #if :id != blank
id = :id and name = :name
-- #fi
;