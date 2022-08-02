/*[q]*/
select *
from test.user
where name in (
    -- #for user of :users filter ${user.name} <> blank && ${user.name} !~ 'j'
    ${:user.name}
    -- #end
    );

/*[q2]*/
select * from test.user t where
--#if :names <> blank
    -- #for name,idx of :names delimiter ' and ' filter ${idx} > 0 && ${name} ~ 'o'
    t.name = ${:name}
    -- #end
--#fi
;

-- #for item[,idx] of :list [delimiter ','] [filter ${item.name} <> blank]