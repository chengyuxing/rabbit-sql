/**
* SQL文件内容格式说明
* 文件格式为标准的sql文件格式进行了一点扩展
* sql名和sql必须满足成对的出现，形如 a b a b a b a b ...
* a为sql名,b为sql（可多行），a与b之间可以有注释
**/
-- aaaaaaaaaaaaaaa
-- bbbbbbbbbbbbbbb

/*[other]*/
/*cccccccccccccc*/
/*xxxxxxxxxxxxxx*/
update test."user"
set name     = 'cyx', --姓名
    password = '123456' --密码
where id = 5;

/*ddddddd
cmd
ddddddd*/
/*[bbbbbbbbbbb]*/
select * from test.files;

a.id, a.name, a.date;

/*[ooooooooo]*/
select count(*),
       count(*) filter ( where grade > 90 )               greate,
       count(*) filter ( where grade < 90 and grade > 60) good,
       count(*) filter ( where grade < 60 )               bad
from test.score;