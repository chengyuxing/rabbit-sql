/*[queryList]*/
select t.guid                     id,
       t.zsxm                     xm,
       t.sfzh,
       zpur.rzjgdm                dwdm,
       zpur.rzjgmc                dwmc,
       (select listagg(r.role_id, ',') within
           group ( order by 1)
        from zhag_ptyw_user_role r
        where r.user_id = t.sfzh) roles
from zhag_ptyw_user t
         left join zhag.zhag_ptyw_user_rzdwxx zpur
                   on t.mjbh = zpur.mjbh and zpur.ifmr = '1'
where
-- //TEMPLATE-BEGIN:cnd1
    1 = 1
-- #if :token != blank
  and t.sfzh = :token
-- #fi
-- #if :username != blank
  and t.mjbh = :username
-- #fi
-- #if :password != blank
  and t.yhmm = :password
-- #fi
-- //TEMPLATE-END
${order}
;

/*[queryCount]*/
select count(*) from zhag_ptyw_user t where ${cnd1}
;

/*{order}*/
order by id
;