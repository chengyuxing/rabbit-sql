/*[a]*/
select now;
current_date

/*[b]*/
/*#查询一组数字#*/
select 1,
       2,
       3,
       4,
       5,
       6

/*#sjd#*/
/*[c]*/
select test.user where id = 10;

/*[queryUserById]*/
/*#根据id查询用户#*/
select 123
;

/*#查询机构信息#*/
/*[dkdkdk]*/
select 'aaa'
;

/*[newParamMode]*/
select *
from test.sum(1, 12)
where 1 = :{id|date(yyyy-mm-dd,12,3.14)|upper|trim}
and o = 10
 and a = 90
  and b = 89
  and name = :{name}
   and age > 0
   and id != 1
   and age <> 90
   and address = '1111'
;