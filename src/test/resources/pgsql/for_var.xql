/*
 Created by IntelliJ IDEA Rabbit SQL plugin.
 User: chengyuxing
 Date: 2025/11/16
 Time: 13:24
 Typing "xql" keyword to get suggestions,
 e.g. "xql:new" will create a sql fragment.
*/

/*[query]*/
select * from users where id in (
    -- #for user of :users delimiter ','
        -- #for address of :user.addresses delimiter ','
            -- #var city = :address.city | upper
            -- #var cityL = :address.city | lower
            :city, :cityL, :list, :address.city, :user.addresses.0.city
            -- #if :cityL = beijing
                'You choose Beijing'
            -- #fi
        -- #done
    -- #done
        )
;
