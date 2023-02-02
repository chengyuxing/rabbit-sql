/*[q]*/
select *
from test.user
where name in (
    -- #for user,idx of :users filter ${ user } <> blank && ${ user.name } !~ 'j'
    ${ :user.name } = ${user.age} > ${idx}
    -- #end
    );

/*[q2]*/
select *
from test.user t
where
--#if :names <> blank
-- #for name,idx of :names delimiter ' and ' filter ${idx} > 0 && ${name} ~ 'o'
t.name = ${:name}
-- #end
--#fi
;

/*[pipe]*/
select *
from test.user t
where
--#if :idCard | is_id_card == true
t.id = :idCard
--#fi
;

/*[switch]*/
select *
from test.user t
where
-- #switch :name | upper
    -- #case 'CYX'
    t.id = :id
    -- #break
    -- #default
    t.id = 10
    -- #break
-- #end
;