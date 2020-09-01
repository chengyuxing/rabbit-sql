/*aaaa*/
/*[query]*/
select ${field}
from test."user" t --用户表
    ${x.part1};

/*插入语句。*/
/*  [   great.insert  ]  */
insert into test."user" (id, name, password)
values (:id, :name, :password);

/*第一部分*/
/*{x.part1}  */
where id = :id
${order};

/*{field}*/
/*用户表字段*/
t.id,t.name,t.password;

/*{order}*/
/*排序*/
order by id;

/*[select_user]*/
select *
from test.user where id < :id;

/*[fruit]*/
select *
from test.student where ${cnd};

/*[fruitCount]*/
select count(*)
from test.student;

/*[logical]*/
select *
from test.student t
WHERE
--#if :age !=null
t.age > 21
--#fi
--#if :name != null
and t.name ~ :name
--#fi
--#if :age <> blank && :age < 90
and age < 90
--#fi
;

/* [ update ] */
update test.user
set
--#if :name <> blank
name = :name,
--#fi
--#if :age <100
age = :age,
--#fi
--#if :address != null
address = :address
--#fi
where id = 10
;
