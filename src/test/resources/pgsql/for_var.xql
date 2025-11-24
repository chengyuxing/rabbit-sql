/*
 Created by IntelliJ IDEA Rabbit SQL plugin.
 User: chengyuxing
 Date: 2025/11/16
 Time: 13:24
 Typing "xql" keyword to get suggestions,
 e.g. "xql:new" will create a sql fragment.
*/

/*[query]*/
-- #check :users = blank throw 'param users must not be null!'
select * from user where id in (
    -- #for user of :users delimiter ','
        -- #for address of :user.addresses delimiter ','
            -- #var city = :address.city | upper
            :city, :another, :user.addresses.0.city
            -- #if :city = KUNMING
               ,'You choose KM'
            -- #fi
        -- #done
    -- #done
        )
;
