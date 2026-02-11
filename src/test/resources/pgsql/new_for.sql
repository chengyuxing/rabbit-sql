/*[queryTemp]*/
select *
from ${db}.${tableName} limit 4
;

/*[insert]*/
insert into test.guest (name, age, address)
values (
   -- #for item of :users; first as isFirst
   -- #if :isFirst
   :item
   -- #else
   ,:item
   -- #fi
   -- #done
)
;

/*[update]*/
update test.user
set
-- #for item of :sets | kv; first as isFirst
-- #if :isFirst
${item.key} = :item.value
-- #else
,${item.key} = :item.value
-- #fi
-- #done
where id = :id;

/*[var]*/
select *
from test.user
where id = 3
-- #if :_databaseId == 'postgresql'
   or id = 9
-- #fi
;

/*[qqq]*/
select *
from test.guest where
-- #if :id != blank
    id = :id
-- #fi
${orderBy}
;

/*[queryAllGuests]*/
select *
from test.guest limit :limit
offset :offset
;

/*[queryAllGuestsCount]*/
select count(*)
from test.guest
;

/*[queryOneGuest]*/
select *
from test.guest
;

/*[maven_dependencies_query]*/
{call test.mvn_dependencies_query(:keywords)}
;

/*[sum]*/
{:res = call test.sum(:a, :b)}
;

/*[new-dynamic]*/
-- #check :age > 30 throw '年龄不能大于30岁'
-- #var safeAge = :age
-- #var id = 14
-- #var users='a,xxx,c' | split(',')
select * from test.guest where id = :id
or name in (
    -- #for item of :users; first as isFirst
        -- #if :isFirst
            :item
        -- #else
           ,:item
        -- #fi
    -- #done
        )
or address = :user.addresses[1]
and age < :safeAge
;