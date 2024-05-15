/*
 Get more information from:
 https://github.com/chengyuxing/rabbit-sql#Prepare-SQL
 https://github.com/chengyuxing/rabbit-sql#xqlfilemanager
 ###
 You can write some description for the file at here.
 ###
*/

/*#some description...#*/
/*[query]*/
/*#more
  description...
  #*/
select *
from test."user" t ${part1};

/*{part1}*/
where id = :id
${order};

/*{order}*/
order by id;

/*[for]*/
select *
from test.user
where id = 1
-- #for id of :ids delimiter ', ' open ' or id in (' close ')'
      -- #if :id > 0
    :_for.id
-- #fi
-- #done
;