/*[query]*/
select *
from test.region
where ${cnd}${order};

/*{cnd}*/
--#if :id <> null
id = :id
--#fi
--#if :pid <> blank
    and pid = :pid
--#fi
;

/*{order}*/
--#if :id <> null
order by id desc
--#fi
;