/*
 Created by IntelliJ IDEA Rabbit SQL plugin.
 User: chengyuxing
 Date: 2025/11/16
 Time: 13:24
 Typing "xql" keyword to get suggestions,
 e.g. "xql:new" will create a sql fragment.
*/

/*[queryGuests]*/
/*#查询访客#*/
-- @cache 30m
-- @rules admin,guest
-- #check :age > 30 throw '年龄不能大于30岁'
-- #var id = 14
-- #var users = 'a,xxx,c' | split(',')
select * from test.guest where
-- //TEMPLATE-BEGIN:myCnd
    id = :id
    and name in (
        -- #for item of :users; last as isLast
        -- #if !:isLast
        :item,
        -- #else
        :item
        -- #fi
        -- #done
        )
-- //TEMPLATE-END
;
;
