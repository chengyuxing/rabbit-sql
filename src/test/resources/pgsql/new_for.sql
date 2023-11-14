/*[query]*/
select *
from test.user
where id = 1
-- #for id of :ids delimiter ', ' open ' or id in (' close ')'
      -- #if :id | isOdd == true
    :_for.id
-- #fi
-- #done
;

/*[queryTemp]*/
select * from ${db}.${tableName} limit 4
;

/*[insert]*/
insert into test.user (name, age, address, dt)
values (
        -- #for item of :users delimiter ', '
            -- #if :item <> blank
            :_for.item
            -- #fi
        -- #done
       )
;

/*[update]*/
update test.user
set
-- #for pair of :data | pairs delimiter ','
${pair.item1} = :_for.pair.item2
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