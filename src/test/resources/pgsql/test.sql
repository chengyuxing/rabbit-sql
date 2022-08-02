/*[me]*/
select ${fields}, '${fields}', '${db}'
from ${db}.pg_aggregate
where
    -- #choose
          -- #when :num <> blank
            id = :num
          -- #break
          -- #when :id = blank
            id = 14
          -- #break
          -- #default
            id = 10
          -- #break
    -- #end
;

/*{fields}*/
id,name,age;