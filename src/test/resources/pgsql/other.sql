/**
* SQL文件内容格式说明
* 文件格式为标准的sql文件格式进行了一点扩展
* sql名和sql必须满足成对的出现，形如 a b a b a b a b ...
* a为sql名,b为sql（可多行），a与b之间可以有注释
**/


/*[other]*/
update test."user"
set name     = 'cyx',
    password = '123456'
where id = 5;
