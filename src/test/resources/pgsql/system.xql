/*
* Created by IntelliJ IDEA.
* User: chengyuxing
* Date: 2023/12/28
* Time: 16:34
* Typing "xql" keyword to get suggestions,
* e.g: "xql:new" will be create a sql fragment.
*/

/*[queryUriRoles]*/
select mu.uri as uri, rm.role_id as role
from zhag_ptyw_role_menu rm
         inner join zhag_ptyw_menu_uri mu on rm.menu_id = mu.menu_id
;

/*[queryUserByPassword]*/
select t.guid                     id,
       t.zsxm                     xm,
       t.sfzh,
       zpur.rzjgdm                dwdm,
       zpur.rzjgmc                dwmc,
       (select listagg(r.role_id, ',') within group ( order by 1)
        from zhag_ptyw_user_role r
        where r.user_id = t.sfzh) roles
from zhag_ptyw_user t
         left join zhag.zhag_ptyw_user_rzdwxx zpur on t.mjbh = zpur.mjbh and zpur.ifmr = '1'
where
-- #if :token != blank
   and t.sfzh = :token
-- #fi
-- #if :username != blank
   and t.mjbh = :username
-- #fi
-- #if :password != blank
  and t.yhmm = :password
-- #fi
;

/*[getModulesByUser]*/
select menu_id, m.title, m.path
from (select rm.menu_id
      from zhag_ptyw_role_menu rm
      where rm.role_id in (select ur.role_id
                           from zhag_ptyw_user_role ur
                           where ur.user_id = :sfzh)) res
         left join zhag_ptyw_menu m
                   on res.menu_id = m.id
where m.jlbz = '1'
  and m.module = '1'
order by m.sort
;

/*[getMyMenus]*/
select m.id,
       m.title,
       m.path,
       m.home,
       m.icon,
       m.fid,
       m.parent,
       m.sort
from (select rm.menu_id
      from zhag_ptyw_role_menu rm
      where rm.role_id in (select ur.role_id
                           from zhag_ptyw_user_role ur
                           where ur.user_id = :sfzh)) res
         left join zhag_ptyw_menu m
                   on res.menu_id = m.id
where m.jlbz = '1'
  and m.module = '0'
  and m.fid like :type || '/%'
order by m.sort
;