/*[query]*/
select * from test.user where id = 1
-- #for id of :ids delimiter ', \n' open ' or id in (' close ')'
    -- #if :id | isOdd == true
    :_for.id
    -- #fi
-- #done
;

/*[update]*/
update test.user
set
-- #for pair of :data | pairs delimiter ',\n'
    ${pair.item1} = :_for.pair.item2
-- #done
where id = :id;

/*[var]*/
select * from test.user
where id = 3
-- #if :_databaseId == 'postgresql'
    or id = 9
-- #fi
;