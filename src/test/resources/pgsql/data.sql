/*aaaa*/
/*[query]*/
select ${field}
from ${db}."user" t --用户表
    ${x.part1};

/*[query2]*/
select current_timestamp;

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
from test.user
where id < :id;

/*[fruit]*/
select *
from test.student
where ${cnd};

/*[update2]*/
select count(*)
from test.student;

/*[logical]*/
/*我的注释*/
select count(*)
from test.student t
WHERE
--#if :age !=null
    t.age > 21
--#fi
--#if :name != null
  and t.name ~ :name
--#fi
name = 'cyx'
--#if :age <> blank && :age < 90
    and age < 90
--#fi
;

/* [ update ] */
update test.user
set
--#if :name <> blank
name    = :name,
--#fi

--#choose
    --#when :age <100
    age     = :age,
    --#break
    --#when :age > 100
    age     = 100,
    --#break
    --#when :age > 150
    age     = 101,
    --#break
--#end

--#if :open <> ''
family  = 'happy',
--#fi

--#choose
    --#when :address == 'kunming'
        address = 'kunming'
    --#break
    --#when :address == "beijing"
        address = '北京'
    --#break
    -- #default
        address = 'Other'
    -- #break
--#end
where id = 10;

/*[custom_paged]*/
select *
from test.region
where id > :id limit :limit
offset :offset;