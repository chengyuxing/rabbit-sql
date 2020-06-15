/*aaaa*/
/*[query]*/
select ${field}
from test."user" t --用户表
    ${part1};

/*插入语句。*/
/*[insert]*/
insert into test."user" (id, name, password)
values (:id, :name, :password);

/*第一部分*/
/*{part1}*/
where id = :id
${order};

/*{field}*/
/*用户表字段*/
t.id,t.name,t.password;

/*{order}*/
/*排序*/
order by id;

/*[select_all]*/
select *
from test.user;

/*[fruit]*/
select *
from test.student;

/*[fruitCount]*/
select count(*) from test.student;