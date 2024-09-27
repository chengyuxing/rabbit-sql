/*[query]*/
select *
from test.user
where id = 1
-- #for id, idx of :ids delimiter ', ' open ' or id in (' close ')'
      -- #if :id | isOdd == true
    :id, :idx
-- #fi
-- #done
;

/*[queryTemp]*/
select *
from ${db}.${tableName} limit 4
;

/*[insert]*/
insert into test.user (name, age, address, dt)
values (
           -- #for item of :users delimiter ', '
           -- #if :item <> blank
           :item
           -- #fi
           -- #done
       )
;

/*[update]*/
update test.user
set
-- #for item of :sets | kv delimiter ', '
${item.key} = :item.value
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
from test.user;

/*[queryAllGuests]*/
select *
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