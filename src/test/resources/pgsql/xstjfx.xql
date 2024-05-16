/*
 Created by IntelliJ IDEA.
 User: Administrator
 Date: 2024/04/26
 Time: 11:09:25
 Typing "xql" keyword to get suggestions,
 e.g: "xql:new" will be create a sql fragment.
*/
/*[qzcstj-jc]*/
/*#强制措施拘传统计#*/
select *
from (
-- #if :dwjb = 40
         select gx.sjjgdm,
                gx.sjjgmc,
                substr(gx.sjjgdm, 1, 6) as      dwdm,
                '合计'                  as      dwmc,
                gx.px,
                count(t.jczbh)                  zs,
                count(t.jczbh)                  zxs,
                0                               wzxs,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_jlzxx jl
                                      where jl.lrsj > t.lrsj
                                        and jl.ajywh = t.ajywh
                                        and jl.fzxyrbh = t.fzxyrbh
                                        and jl.jlbz = '1')
                              then t.jczbh end) jchxj,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_dbzxx db
                                      where db.lrsj > t.lrsj
                                        and db.ajywh = t.ajywh
                                        and db.fzxyrbh = t.fzxyrbh
                                        and db.jlbz = '1')
                              then t.jczbh end) jchdb,
                count(case
                          when exists(select 1
                                      from g2bajd.ZFBA_GT_CQBGS b
                                      where wslx = '3093'
                                        and b.lrsj > t.lrsj
                                        and b.ajywh = t.ajywh
                                        and b.jlbz = '1')
                              then t.jczbh end) jchzlxz
         from g2bajd.zfba_xs_jczxx t
                  join (select *
                        from zhag_sys_jggx
                        where sjjgjz = '38'
                          and sjjgjb <> '40'
                          and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                       on gx.dm = t.lrdwdm
         where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
           and t.jlbz = '1'
           -- #if :dwjb != 40
           and gx.SJJGDM = :sjjgdm
           -- #fi
           and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
         group by gx.sjjgdm, gx.sjjgmc, gx.px
         union all
-- #fi
         select '9999' as                       sjjgdm,
                '总数' as                       sjjgmc,
                '5301' as                       dwdm,
                ''     as                       dwmc,
                '9999' as                       px,
                count(t.jczbh)                  zs,
                count(t.jczbh)                  zxs,
                0                               wzxs,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_jlzxx jl
                                      where jl.lrsj > t.lrsj
                                        and jl.ajywh = t.ajywh
                                        and jl.fzxyrbh = t.fzxyrbh)
                              then t.jczbh end) jchxj,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_dbzxx db
                                      where db.lrsj > t.lrsj
                                        and db.ajywh = t.ajywh
                                        and db.fzxyrbh = t.fzxyrbh)
                              then t.jczbh end) jchdb,
                count(case
                          when exists(select 1
                                      from g2bajd.ZFBA_GT_CQBGS b
                                      where wslx = '3093'
                                        and b.lrsj > t.lrsj
                                        and b.ajywh = t.ajywh)
                              then t.jczbh end) jchzlxz
         from g2bajd.zfba_xs_jczxx t
                  join (select *
                        from zhag_sys_jggx
                        where sjjgjz = '38'
                          and sjjgjb <> '40'
                          and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                       on gx.dm = t.lrdwdm
         where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
           and t.jlbz = '1'
           and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
         union all
         select gx.sjjgdm,
                gx.sjjgmc,
                t.lrdwdm,
                nvl(jg.jc, jg.mc) as            jjdwmc,
                gx.px,
                count(t.jczbh)                  zs,
                count(t.jczbh)                  zxs,
                0                               wzxs,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_jlzxx jl
                                      where jl.lrsj > t.lrsj
                                        and jl.ajywh = t.ajywh
                                        and jl.fzxyrbh = t.fzxyrbh)
                              then t.jczbh end) jchxj,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_dbzxx db
                                      where db.lrsj > t.lrsj
                                        and db.ajywh = t.ajywh
                                        and db.fzxyrbh = t.fzxyrbh)
                              then t.jczbh end) jchdb,
                count(case
                          when exists(select 1
                                      from g2bajd.ZFBA_GT_CQBGS b
                                      where wslx = '3093'
                                        and b.lrsj > t.lrsj
                                        and b.ajywh = t.ajywh)
                              then t.jczbh end) jchzlxz
         from g2bajd.zfba_xs_jczxx t
                  join (select *
                        from zhag_sys_jggx
                        where sjjgjz = '38'
                          and sjjgjb <> '40'
                          and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                       on gx.dm = t.lrdwdm
                  left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
         where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
           and t.jlbz = '1'
           -- #if :dwjb != 40
           and gx.SJJGDM = :sjjgdm
           -- #fi
           and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
         group by gx.sjjgdm, gx.sjjgmc, gx.px, t.lrdwdm, nvl(jg.jc, jg.mc))
order by px, dwdm
;

/*[qzcstj-jcdetails]*/
/*#强制措施拘传统计详情数据#*/
select *
from g2bajd.zfba_xs_jczxx t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  and t.jlbz = '1'
  -- #if :sjjgdm != '9999'
  and gx.SJJGDM = :sjjgdm
  -- #fi
  -- #switch :lx
  -- #case 'zs'
  and 1 = 1
  -- #break
  -- #case 'zxs'
  and 1 = 1
  -- #break
  -- #case 'wzxs'
  and 1 = 2
  -- #break
  -- #case 'jchxj'
  and exists(select 1
             from g2bajd.zfba_xs_jlzxx jl
             where jl.lrsj > t.lrsj
               and jl.ajywh = t.ajywh
               and jl.fzxyrbh = t.fzxyrbh)
-- #break
  -- #case 'jchdb'
  and exists(select 1
             from g2bajd.zfba_xs_dbzxx db
             where db.lrsj > t.lrsj
               and db.ajywh = t.ajywh
               and db.fzxyrbh = t.fzxyrbh)
-- #break
  -- #case 'jchzlxz'
  and exists(select 1
             from g2bajd.ZFBA_GT_CQBGS b
             where wslx = '3093'
               and b.lrsj > t.lrsj
               and b.ajywh = t.ajywh)
-- #break
-- #end
  and gx.dm like :dwdm || '%'
order by t.lrsj desc;

/*[qzcstj-xj]*/
/*#强制措施刑拘统计数据#*/
select *
from (
-- #if :dwjb = 40
select gx.sjjgdm,
             gx.sjjgmc,
             substr(gx.sjjgdm, 1, 6) as                                                          dwdm,
             '合计' as dwmc,
             gx.px,
             count(t.jlzbh)                                                                      zs,
             count(case when t.SFSSZX = '1' then t.jlzbh end)                                    zxhzs,
             count(case
                       when exists(select 1
                                   from g2kss.kss_aryrs r
                                   where r.ajbh = t.ajbh
                                     and r.xyrbh = t.fzxyrbh
                                     and r.jlbz = '1'
                                     and r.rsyy = '11'
                                     and r.C_CZSJ > t.lrsj) then t.jlzbh end)                    rksss,
             count(case
                       when t.SFSSZX = '0' and exists(select 1
                                                      from g2bajd.zfba_gt_xyrztxx zt
                                                      where zt.WFFZXYRBH = t.fzxyrbh
                                                        and zt.jlbz = '1') then t.jlzbh end)     xjzts,
             count(case
                       when t.SFSSZX = '0' and not exists(select 1
                                                          from g2bajd.zfba_gt_xyrztxx zt
                                                          where zt.WFFZXYRBH = t.fzxyrbh
                                                            and zt.jlbz = '1') then t.jlzbh end) qtwzxs,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_dbzxx db
                                   where db.lrsj > t.lrsj
                                     and db.jlbz = '1'
                                     and db.ajywh = t.ajywh
                                     and db.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhdb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_qbhsjdsxx qb
                                   where qb.lrsj > t.lrsj
                                     and qb.jlbz = '1'
                                     and qb.ajywh = t.ajywh
                                     and qb.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhqb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jsjzjdsxx jj
                                   where jj.lrsj > t.lrsj
                                     and jj.jlbz = '1'
                                     and jj.ajywh = t.ajywh
                                     and jj.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhjj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_sftzsxx sf
                                   where sf.lrsj > t.lrsj
                                     and sf.jlbz = '1'
                                     and sf.ajywh = t.ajywh
                                     and sf.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhsf,
             count(case
                       when exists(select 1
                                   from g2bajd.ZFBA_GT_CQBGS b
                                   where wslx = '3093'
                                     and b.jlbz = '1'
                                     and b.lrsj > t.lrsj
                                     and b.ajywh = t.ajywh)
                           then t.jlzbh end)                                                     xjhzlxz
      from g2bajd.zfba_xs_jlzxx t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
      where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
        and t.jlbz = '1'
        -- #if :dwjb != 40
        and gx.SJJGDM = :sjjgdm
        -- #fi
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      group by gx.sjjgdm, gx.sjjgmc, gx.px
      union all
-- #fi
      select '9999' as                       sjjgdm,
             '总数' as                       sjjgmc,
             '5301' as                       dwdm,
             ''     as                       dwmc,
             '9999' as                       px,
             count(t.jlzbh)                                                                      zs,
             count(case when t.SFSSZX = '1' then t.jlzbh end)                                    zxhzs,
             count(case
                       when exists(select 1
                                   from g2kss.kss_aryrs r
                                   where r.ajbh = t.ajbh
                                     and r.xyrbh = t.fzxyrbh
                                     and r.jlbz = '1'
                                     and r.rsyy = '11'
                                     and r.C_CZSJ > t.lrsj) then t.jlzbh end)                    rksss,
             count(case
                       when t.SFSSZX = '0' and exists(select 1
                                                      from g2bajd.zfba_gt_xyrztxx zt
                                                      where zt.WFFZXYRBH = t.fzxyrbh
                                                        and zt.jlbz = '1') then t.jlzbh end)     xjzts,
             count(case
                       when t.SFSSZX = '0' and not exists(select 1
                                                          from g2bajd.zfba_gt_xyrztxx zt
                                                          where zt.WFFZXYRBH = t.fzxyrbh
                                                            and zt.jlbz = '1') then t.jlzbh end) qtwzxs,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_dbzxx db
                                   where db.lrsj > t.lrsj
                                     and db.jlbz = '1'
                                     and db.ajywh = t.ajywh
                                     and db.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhdb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_qbhsjdsxx qb
                                   where qb.lrsj > t.lrsj
                                     and qb.jlbz = '1'
                                     and qb.ajywh = t.ajywh
                                     and qb.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhqb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jsjzjdsxx jj
                                   where jj.lrsj > t.lrsj
                                     and jj.jlbz = '1'
                                     and jj.ajywh = t.ajywh
                                     and jj.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhjj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_sftzsxx sf
                                   where sf.lrsj > t.lrsj
                                     and sf.jlbz = '1'
                                     and sf.ajywh = t.ajywh
                                     and sf.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhsf,
             count(case
                       when exists(select 1
                                   from g2bajd.ZFBA_GT_CQBGS b
                                   where wslx = '3093'
                                     and b.jlbz = '1'
                                     and b.lrsj > t.lrsj
                                     and b.ajywh = t.ajywh)
                           then t.jlzbh end)                                                     xjhzlxz
      from g2bajd.zfba_xs_jlzxx t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
      where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
        and t.jlbz = '1'
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      union all
      select gx.sjjgdm,
             gx.sjjgmc,
             t.lrdwdm,
             nvl(jg.jc, jg.mc) as                                                                jjdwmc,
             gx.px,
             count(t.jlzbh)                                                                      zs,
             count(case when t.SFSSZX = '1' then t.jlzbh end)                                    zxhzs,
             count(case
                       when exists(select 1
                                   from g2kss.kss_aryrs r
                                   where r.ajbh = t.ajbh
                                     and r.xyrbh = t.fzxyrbh
                                     and r.jlbz = '1'
                                     and r.rsyy = '11'
                                     and r.C_CZSJ > t.lrsj) then t.jlzbh end)                    rksss,
             count(case
                       when t.SFSSZX = '0' and exists(select 1
                                                      from g2bajd.zfba_gt_xyrztxx zt
                                                      where zt.WFFZXYRBH = t.fzxyrbh
                                                        and zt.jlbz = '1') then t.jlzbh end)     xjzts,
             count(case
                       when t.SFSSZX = '0' and not exists(select 1
                                                          from g2bajd.zfba_gt_xyrztxx zt
                                                          where zt.WFFZXYRBH = t.fzxyrbh
                                                            and zt.jlbz = '1') then t.jlzbh end) qtwzxs,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_dbzxx db
                                   where db.lrsj > t.lrsj
                                     and db.jlbz = '1'
                                     and db.ajywh = t.ajywh
                                     and db.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhdb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_qbhsjdsxx qb
                                   where qb.lrsj > t.lrsj
                                     and qb.jlbz = '1'
                                     and qb.ajywh = t.ajywh
                                     and qb.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhqb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jsjzjdsxx jj
                                   where jj.lrsj > t.lrsj
                                     and jj.jlbz = '1'
                                     and jj.ajywh = t.ajywh
                                     and jj.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhjj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_sftzsxx sf
                                   where sf.lrsj > t.lrsj
                                     and sf.jlbz = '1'
                                     and sf.ajywh = t.ajywh
                                     and sf.fzxyrbh = t.fzxyrbh)
                           then t.jlzbh end)                                                     xjhsf,
             count(case
                       when exists(select 1
                                   from g2bajd.ZFBA_GT_CQBGS b
                                   where wslx = '3093'
                                     and b.jlbz = '1'
                                     and b.lrsj > t.lrsj
                                     and b.ajywh = t.ajywh)
                           then t.jlzbh end)                                                     xjhzlxz
      from g2bajd.zfba_xs_jlzxx t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
               left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
      where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
        and t.jlbz = '1'
        -- #if :dwjb != 40
        and gx.SJJGDM = :sjjgdm
        -- #fi
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      group by gx.sjjgdm, gx.sjjgmc, gx.px, t.lrdwdm, nvl(jg.jc, jg.mc)
     )
order by px, dwdm
;

/*[qzcstj-xjdetail]*/
/*#强制措施刑拘统计详情数据#*/
select t.*
from g2bajd.zfba_xs_jlzxx t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  and t.jlbz = '1'
  -- #if :sjjgdm != '9999'
  and gx.SJJGDM = :sjjgdm
  -- #fi
  -- #switch :lx
  -- #case 'zs'
  and 1 = 1
  -- #break
  -- #case 'zxhzs'
  and t.SFSSZX = '1'
  -- #break
  -- #case 'rksss'
  and exists(select 1
             from g2kss.kss_aryrs r
             where r.ajbh = t.ajbh
               and r.xyrbh = t.fzxyrbh
               and r.jlbz = '1'
               and r.rsyy = '11'
               and r.C_CZSJ > t.lrsj)
  -- #break
  -- #case 'xjzts'
  and t.SFSSZX = '0'
  and exists(select 1
             from g2bajd.zfba_gt_xyrztxx zt
             where zt.WFFZXYRBH = t.fzxyrbh
               and zt.jlbz = '1')
  -- #break
  -- #case 'qtwzxs'
  and t.SFSSZX = '0'
  and not exists(select 1
                 from g2bajd.zfba_gt_xyrztxx zt
                 where zt.WFFZXYRBH = t.fzxyrbh
                   and zt.jlbz = '1')
  -- #break
  -- #case 'xjhqb'
  and exists(select 1
             from g2bajd.zfba_xs_qbhsjdsxx qb
             where qb.lrsj > t.lrsj
               and qb.ajywh = t.ajywh
               and qb.fzxyrbh = t.fzxyrbh)
-- #break
  -- #case 'xjhjj'
  and exists(select 1
             from g2bajd.zfba_xs_jsjzjdsxx jj
             where jj.lrsj > t.lrsj
               and jj.ajywh = t.ajywh
               and jj.fzxyrbh = t.fzxyrbh)
-- #break
  -- #case 'xjhdb'
  and exists(select 1
             from g2bajd.zfba_xs_dbzxx db
             where db.lrsj > t.lrsj
               and db.ajywh = t.ajywh
               and db.fzxyrbh = t.fzxyrbh)
-- #break
  -- #case 'xjhsf'
  and exists(select 1
             from g2bajd.zfba_xs_sftzsxx sf
             where sf.lrsj > t.lrsj
               and sf.jlbz = '1'
               and sf.ajywh = t.ajywh
               and sf.fzxyrbh = t.fzxyrbh)
  -- #break
  -- #case 'xjhzlxz'
  and exists(select 1
             from g2bajd.ZFBA_GT_CQBGS b
             where wslx = '3093'
               and b.lrsj > t.lrsj
               and b.ajywh = t.ajywh)
-- #break
-- #end
  and gx.dm like :dwdm || '%'
order by t.lrsj desc;

/*[qzcstj-jj]*/
/*#强制措施监视居住统计数据#*/
select *
from (
-- #if :dwjb = 40
select gx.sjjgdm,
             gx.sjjgmc,
             substr(gx.sjjgdm, 1, 6) as                                 dwdm,
             '合计' as dwmc,
             gx.px,
             count(case when t.lx = '1' then t.bh end)                  zs,
             count(case when t.lx = '1' and t.zdjs = '0' then t.bh end) ybjss,
             count(case when t.lx = '1' and t.zdjs = '1' then t.bh end) zdzss,
             count(case when t.lx = '2' then t.bh end)                  jczs,
             count(case when t.lx = '2' and t.zdjs = '0' then t.bh end) jcybjss,
             count(case when t.lx = '2' and t.zdjs = '1' then t.bh end) jczdzss,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jlzxx jl
                                   where jl.lrsj > t.lrsj
                                     and jl.jlbz = '1'
                                     and t.lx = '1'
                                     and jl.ajywh = t.ajywh
                                     and jl.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               jjhxj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_dbzxx db
                                   where db.lrsj > t.lrsj
                                     and db.jlbz = '1'
                                     and t.lx = '1'
                                     and db.ajywh = t.ajywh
                                     and db.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               jjhdb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_qsyjsxx qs
                                   where qs.lrsj > t.lrsj
                                     and qs.jlbz = '1'
                                     and t.lx = '1'
                                     and qs.ajywh = t.ajywh
                                     and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               jjhqs,
             count(case
                       when not exists(select 1
                                       from g2bajd.zfba_xs_qsyjsxx qs
                                       where qs.lrsj > t.lrsj
                                         and qs.jlbz = '1'
                                         and t.lx = '1'
                                         and qs.ajywh = t.ajywh
                                         and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               jjhwqs
      from (select t1.JSJZJDSBH as bh,
                   t1.ajbh,
                   t1.ajywh,
                   t1.fzxyrbh,
                   t1.ZDJS,
                   t1.lrdwdm,
                   t1.lrsj,
                   '1'             lx
            from g2bajd.zfba_xs_jsjzjdsxx t1
            where t1.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t1.jlbz = '1'
            union all
            select t2.JCJSJZJDSBH,
                   t2.ajbh,
                   t2.ajywh,
                   t2.fzxyrbh,
                   js.zdjs,
                   t2.lrdwdm,
                   t2.lrsj,
                   '2' lx
            from g2bajd.zfba_xs_jcjsjzjdsxx t2
                     left join g2bajd.ZFBA_XS_JSJZJDSXX js on js.FZXYRBH = t2.fzxyrbh
            where t2.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t2.jlbz = '1') t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
      where 1 = 1
        -- #if :dwjb != 40
        and gx.SJJGDM = :sjjgdm
        -- #fi
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      group by gx.sjjgdm, gx.sjjgmc, gx.px
      union all
-- #fi
      select '9999' as                       sjjgdm,
             '总数' as                       sjjgmc,
             '5301' as                       dwdm,
             ''     as                       dwmc,
             '9999' as                       px,
             count(case when t.lx = '1' then t.bh end)                  zs,
             count(case when t.lx = '1' and t.zdjs = '0' then t.bh end) ybjss,
             count(case when t.lx = '1' and t.zdjs = '1' then t.bh end) zdzss,
             count(case when t.lx = '2' then t.bh end)                  jczs,
             count(case when t.lx = '2' and t.zdjs = '0' then t.bh end) jcybjss,
             count(case when t.lx = '2' and t.zdjs = '1' then t.bh end) jczdzss,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jlzxx jl
                                   where jl.lrsj > t.lrsj
                                     and jl.jlbz = '1'
                                     and t.lx = '1'
                                     and jl.ajywh = t.ajywh
                                     and jl.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               jjhxj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_dbzxx db
                                   where db.lrsj > t.lrsj
                                     and db.jlbz = '1'
                                     and t.lx = '1'
                                     and db.ajywh = t.ajywh
                                     and db.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               jjhdb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_qsyjsxx qs
                                   where qs.lrsj > t.lrsj
                                     and qs.jlbz = '1'
                                     and t.lx = '1'
                                     and qs.ajywh = t.ajywh
                                     and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               jjhqs,
             count(case
                       when not exists(select 1
                                       from g2bajd.zfba_xs_qsyjsxx qs
                                       where qs.lrsj > t.lrsj
                                         and qs.jlbz = '1'
                                         and t.lx = '1'
                                         and qs.ajywh = t.ajywh
                                         and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               jjhwqs
      from (select t1.JSJZJDSBH as bh,
                   t1.ajbh,
                   t1.ajywh,
                   t1.fzxyrbh,
                   t1.ZDJS,
                   t1.lrdwdm,
                   t1.lrsj,
                   '1'             lx
            from g2bajd.zfba_xs_jsjzjdsxx t1
            where t1.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t1.jlbz = '1'
            union all
            select t2.JCJSJZJDSBH,
                   t2.ajbh,
                   t2.ajywh,
                   t2.fzxyrbh,
                   js.zdjs,
                   t2.lrdwdm,
                   t2.lrsj,
                   '2' lx
            from g2bajd.zfba_xs_jcjsjzjdsxx t2
                     left join g2bajd.ZFBA_XS_JSJZJDSXX js on js.FZXYRBH = t2.fzxyrbh
            where t2.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t2.jlbz = '1') t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
      where 1 = 1
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      union all
      select gx.sjjgdm,
             gx.sjjgmc,
             t.lrdwdm,
             nvl(jg.jc, jg.mc) as                                       jjdwmc,
             gx.px,
             count(case when t.lx = '1' then t.bh end)                  zs,
             count(case when t.lx = '1' and t.zdjs = '0' then t.bh end) ybjss,
             count(case when t.lx = '1' and t.zdjs = '1' then t.bh end) zdzss,
             count(case when t.lx = '2' then t.bh end)                  jczs,
             count(case when t.lx = '2' and t.zdjs = '0' then t.bh end) jcybjss,
             count(case when t.lx = '2' and t.zdjs = '1' then t.bh end) jczdzss,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jlzxx jl
                                   where jl.lrsj > t.lrsj
                                     and jl.jlbz = '1'
                                     and t.lx = '1'
                                     and jl.ajywh = t.ajywh
                                     and jl.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               jjhxj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_dbzxx db
                                   where db.lrsj > t.lrsj
                                     and db.jlbz = '1'
                                     and t.lx = '1'
                                     and db.ajywh = t.ajywh
                                     and db.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               jjhdb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_qsyjsxx qs
                                   where qs.lrsj > t.lrsj
                                     and qs.jlbz = '1'
                                     and t.lx = '1'
                                     and qs.ajywh = t.ajywh
                                     and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               jjhqs,
             count(case
                       when not exists(select 1
                                       from g2bajd.zfba_xs_qsyjsxx qs
                                       where qs.lrsj > t.lrsj
                                         and qs.jlbz = '1'
                                         and t.lx = '1'
                                         and qs.ajywh = t.ajywh
                                         and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               jjhwqs
      from (select t1.JSJZJDSBH as bh,
                   t1.ajbh,
                   t1.ajywh,
                   t1.fzxyrbh,
                   t1.ZDJS,
                   t1.lrdwdm,
                   t1.lrsj,
                   '1'             lx
            from g2bajd.zfba_xs_jsjzjdsxx t1
            where t1.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t1.jlbz = '1'
            union all
            select t2.JCJSJZJDSBH,
                   t2.ajbh,
                   t2.ajywh,
                   t2.fzxyrbh,
                   js.zdjs,
                   t2.lrdwdm,
                   t2.lrsj,
                   '2' lx
            from g2bajd.zfba_xs_jcjsjzjdsxx t2
                     left join g2bajd.ZFBA_XS_JSJZJDSXX js on js.FZXYRBH = t2.fzxyrbh
            where t2.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t2.jlbz = '1') t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
               left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
      where 1 = 1
        -- #if :dwjb != 40
        and gx.SJJGDM = :sjjgdm
        -- #fi
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      group by gx.sjjgdm, gx.sjjgmc, gx.px, t.lrdwdm, nvl(jg.jc, jg.mc)
     )
order by px, dwdm
;

/*[qzcstj-jjdetail]*/
/*#强制措施监视居住统计详情数据#*/
select t.*
from (select t1.JSJZJDSBH as bh,
             t1.JSJZJDSWH as wh,
             t1.ajbh,
             t1.ajywh,
             t1.fzxyrbh,
             t1.ZDJS,
             t1.lrdwdm,
             t1.lrdwmc,
             t1.lrr,
             t1.lrsj,
             '1'             lx
      from g2bajd.zfba_xs_jsjzjdsxx t1
      where t1.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
        and t1.jlbz = '1'
      union all
      select t2.JCJSJZJDSBH,
             t2.JCJSJZJDSWH as wh,
             t2.ajbh,
             t2.ajywh,
             t2.fzxyrbh,
             js.zdjs,
             t2.lrdwdm,
             t2.lrdwmc,
             t2.lrr,
             t2.lrsj,
             '2'               lx
      from g2bajd.zfba_xs_jcjsjzjdsxx t2
               left join g2bajd.ZFBA_XS_JSJZJDSXX js on js.FZXYRBH = t2.fzxyrbh
      where t2.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
        and t2.jlbz = '1') t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
where 1 = 1
  -- #if :sjjgdm != '9999'
  and gx.SJJGDM = :sjjgdm
  -- #fi
  -- #switch :lx
  -- #case 'zs'
  and t.lx = '1'
  -- #break
  -- #case 'ybjss'
  and t.lx = '1'
  and t.zdjs = '0'
  -- #break
  -- #case 'zdzss'
  and t.lx = '1'
  and t.zdjs = '1'
  -- #break
  -- #case 'jczs'
  and t.lx = '2'
  -- #break
  -- #case 'jcybjss'
  and t.lx = '2'
  and t.zdjs = '0'
  -- #break
  -- #case 'jczdzss'
  and t.lx = '2'
  and t.zdjs = '1'
  -- #break
  -- #case 'jjhxj'
  and exists(select 1
             from g2bajd.zfba_xs_jlzxx jl
             where jl.lrsj > t.lrsj
               and jl.jlbz = '1'
               and t.lx = '1'
               and jl.ajywh = t.ajywh
               and jl.fzxyrbh = t.fzxyrbh)
  -- #break

  -- #case 'jjhdb'
  and exists(select 1
             from g2bajd.zfba_xs_dbzxx db
             where db.lrsj > t.lrsj
               and db.jlbz = '1'
               and t.lx = '1'
               and db.ajywh = t.ajywh
               and db.fzxyrbh = t.fzxyrbh)
-- #break
-- #case 'jjhqs'
  and exists(select 1
             from g2bajd.zfba_xs_qsyjsxx qs
             where qs.lrsj > t.lrsj
               and qs.jlbz = '1'
               and t.lx = '1'
               and qs.ajywh = t.ajywh
               and qs.cldxbh like '%' || t.fzxyrbh || '%')
-- #break
-- #case 'jjhwqs'
  and not exists(select 1
                 from g2bajd.zfba_xs_qsyjsxx qs
                 where qs.lrsj > t.lrsj
                   and qs.jlbz = '1'
                   and t.lx = '1'
                   and qs.ajywh = t.ajywh
                   and qs.cldxbh like '%' || t.fzxyrbh || '%')
-- #break
-- #end
  and gx.dm like :dwdm || '%'
order by lrsj desc
;

/*[qzcstj-qb]*/
/*#强制措施取保候审统计#*/
select *
from (
-- #if :dwjb = 40
select gx.sjjgdm,
             gx.sjjgmc,
             substr(gx.sjjgdm, 1, 6) as                                 dwdm,
             '合计' as dwmc,
             gx.px,
             count(case when t.lx = '1' then t.bh end)                  zs,
             count(case when t.lx = '1' and t.qbfs = '1' then t.bh end) rbs,
             count(case when t.lx = '1' and t.qbfs = '2' then t.bh end) cbs,
             count(case when t.lx = '2' then t.bh end)                  jczs,
             count(case when t.lx = '2' and t.qbfs = '1' then t.bh end) jcrbs,
             count(case when t.lx = '2' and t.qbfs = '2' then t.bh end) jccbs,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jlzxx jl
                                   where jl.lrsj > t.lrsj
                                     and jl.jlbz = '1'
                                     and t.lx = '1'
                                     and jl.ajywh = t.ajywh
                                     and jl.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               qbhxj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jsjzjdsxx jj
                                   where jj.lrsj > t.lrsj
                                     and jj.jlbz = '1'
                                     and t.lx = '1'
                                     and jj.ajywh = t.ajywh
                                     and jj.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               qbhjj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_dbzxx db
                                   where db.lrsj > t.lrsj
                                     and db.jlbz = '1'
                                     and t.lx = '1'
                                     and db.ajywh = t.ajywh
                                     and db.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               qbhdb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_qsyjsxx qs
                                   where qs.lrsj > t.lrsj
                                     and qs.jlbz = '1'
                                     and t.lx = '1'
                                     and qs.ajywh = t.ajywh
                                     and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               qbhqs,
             count(case
                       when not exists(select 1
                                       from g2bajd.zfba_xs_qsyjsxx qs
                                       where qs.lrsj > t.lrsj
                                         and qs.jlbz = '1'
                                         and t.lx = '1'
                                         and qs.ajywh = t.ajywh
                                         and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               qbhwqs
      from (select t1.QBHSJDSBH as bh,
                   t1.ajbh,
                   t1.ajywh,
                   t1.fzxyrbh,
                   t1.qbfs,
                   t1.lrdwdm,
                   t1.lrsj,
                   '1'             lx
            from g2bajd.zfba_xs_qbhsjdsxx t1
            where t1.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t1.jlbz = '1'
            union all
            select t2.JCQBHSJDSBH,
                   t2.ajbh,
                   t2.ajywh,
                   t2.fzxyrbh,
                   qb.qbfs,
                   t2.lrdwdm,
                   t2.lrsj,
                   '2' lx
            from g2bajd.zfba_xs_jcqbhsjdsxx t2
                     left join g2bajd.zfba_xs_qbhsjdsxx qb on qb.FZXYRBH = t2.fzxyrbh
            where t2.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t2.jlbz = '1') t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
      where 1 = 1
        -- #if :dwjb != 40
        and gx.SJJGDM = :sjjgdm
        -- #fi
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      group by gx.sjjgdm, gx.sjjgmc, gx.px
      union all
-- #fi
      select '9999' as                       sjjgdm,
             '总数' as                       sjjgmc,
             '5301' as                       dwdm,
             ''     as                       dwmc,
             '9999' as                       px,
             count(case when t.lx = '1' then t.bh end)                  zs,
             count(case when t.lx = '1' and t.qbfs = '1' then t.bh end) rbs,
             count(case when t.lx = '1' and t.qbfs = '2' then t.bh end) cbs,
             count(case when t.lx = '2' then t.bh end)                  jczs,
             count(case when t.lx = '2' and t.qbfs = '1' then t.bh end) jcrbs,
             count(case when t.lx = '2' and t.qbfs = '2' then t.bh end) jccbs,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jlzxx jl
                                   where jl.lrsj > t.lrsj
                                     and jl.jlbz = '1'
                                     and t.lx = '1'
                                     and jl.ajywh = t.ajywh
                                     and jl.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               qbhxj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jsjzjdsxx jj
                                   where jj.lrsj > t.lrsj
                                     and jj.jlbz = '1'
                                     and t.lx = '1'
                                     and jj.ajywh = t.ajywh
                                     and jj.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               qbhjj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_dbzxx db
                                   where db.lrsj > t.lrsj
                                     and db.jlbz = '1'
                                     and t.lx = '1'
                                     and db.ajywh = t.ajywh
                                     and db.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               qbhdb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_qsyjsxx qs
                                   where qs.lrsj > t.lrsj
                                     and qs.jlbz = '1'
                                     and t.lx = '1'
                                     and qs.ajywh = t.ajywh
                                     and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               qbhqs,
             count(case
                       when not exists(select 1
                                       from g2bajd.zfba_xs_qsyjsxx qs
                                       where qs.lrsj > t.lrsj
                                         and qs.jlbz = '1'
                                         and t.lx = '1'
                                         and qs.ajywh = t.ajywh
                                         and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               qbhwqs
      from (select t1.QBHSJDSBH as bh,
                   t1.ajbh,
                   t1.ajywh,
                   t1.fzxyrbh,
                   t1.qbfs,
                   t1.lrdwdm,
                   t1.lrsj,
                   '1'             lx
            from g2bajd.zfba_xs_qbhsjdsxx t1
            where t1.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t1.jlbz = '1'
            union all
            select t2.JCQBHSJDSBH,
                   t2.ajbh,
                   t2.ajywh,
                   t2.fzxyrbh,
                   qb.qbfs,
                   t2.lrdwdm,
                   t2.lrsj,
                   '2' lx
            from g2bajd.zfba_xs_jcqbhsjdsxx t2
                     left join g2bajd.zfba_xs_qbhsjdsxx qb on qb.FZXYRBH = t2.fzxyrbh
            where t2.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t2.jlbz = '1') t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
      where 1 = 1
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      union all
      select gx.sjjgdm,
             gx.sjjgmc,
             t.lrdwdm,
             nvl(jg.jc, jg.mc) as                                       jjdwmc,
             gx.px,
             count(case when t.lx = '1' then t.bh end)                  zs,
             count(case when t.lx = '1' and t.qbfs = '1' then t.bh end) rbs,
             count(case when t.lx = '1' and t.qbfs = '2' then t.bh end) cbs,
             count(case when t.lx = '2' then t.bh end)                  jczs,
             count(case when t.lx = '2' and t.qbfs = '1' then t.bh end) jcrbs,
             count(case when t.lx = '2' and t.qbfs = '2' then t.bh end) jccbs,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jlzxx jl
                                   where jl.lrsj > t.lrsj
                                     and jl.jlbz = '1'
                                     and t.lx = '1'
                                     and jl.ajywh = t.ajywh
                                     and jl.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               qbhxj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_jsjzjdsxx jj
                                   where jj.lrsj > t.lrsj
                                     and jj.jlbz = '1'
                                     and t.lx = '1'
                                     and jj.ajywh = t.ajywh
                                     and jj.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               qbhjj,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_dbzxx db
                                   where db.lrsj > t.lrsj
                                     and db.jlbz = '1'
                                     and t.lx = '1'
                                     and db.ajywh = t.ajywh
                                     and db.fzxyrbh = t.fzxyrbh)
                           then t.bh end)                               qbhdb,
             count(case
                       when exists(select 1
                                   from g2bajd.zfba_xs_qsyjsxx qs
                                   where qs.lrsj > t.lrsj
                                     and qs.jlbz = '1'
                                     and t.lx = '1'
                                     and qs.ajywh = t.ajywh
                                     and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               qbhqs,
             count(case
                       when not exists(select 1
                                       from g2bajd.zfba_xs_qsyjsxx qs
                                       where qs.lrsj > t.lrsj
                                         and qs.jlbz = '1'
                                         and t.lx = '1'
                                         and qs.ajywh = t.ajywh
                                         and qs.cldxbh like '%' || t.fzxyrbh || '%')
                           then t.bh end)                               qbhwqs
      from (select t1.QBHSJDSBH as bh,
                   t1.ajbh,
                   t1.ajywh,
                   t1.fzxyrbh,
                   t1.qbfs,
                   t1.lrdwdm,
                   t1.lrsj,
                   '1'             lx
            from g2bajd.zfba_xs_qbhsjdsxx t1
            where t1.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t1.jlbz = '1'
            union all
            select t2.JCQBHSJDSBH,
                   t2.ajbh,
                   t2.ajywh,
                   t2.fzxyrbh,
                   qb.qbfs,
                   t2.lrdwdm,
                   t2.lrsj,
                   '2' lx
            from g2bajd.zfba_xs_jcqbhsjdsxx t2
                     left join g2bajd.zfba_xs_qbhsjdsxx qb on qb.FZXYRBH = t2.fzxyrbh
            where t2.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and t2.jlbz = '1') t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
               left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
      where 1 = 1
        -- #if :dwjb != 40
        and gx.SJJGDM = :sjjgdm
        -- #fi
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      group by gx.sjjgdm, gx.sjjgmc, gx.px, t.lrdwdm, nvl(jg.jc, jg.mc)
     )
order by px, dwdm
;
/*[qzcstj-qbdetail]*/
/*#强制措施取保候审统计详情#*/
select t.*
from (select t1.QBHSJDSBH as bh,
             t1.ajbh,
             t1.ajywh,
             t1.fzxyrbh,
             t1.qbfs,
             t1.lrdwdm,
             t1.lrdwmc,
             t1.lrr,
             t1.lrsj,
             '1'             lx
      from g2bajd.zfba_xs_qbhsjdsxx t1
      where t1.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
        and t1.jlbz = '1'
      union all
      select t2.JCQBHSJDSBH,
             t2.ajbh,
             t2.ajywh,
             t2.fzxyrbh,
             qb.qbfs,
             t2.lrdwdm,
             t2.lrdwmc,
             t2.lrr,
             t2.lrsj,
             '2' lx
      from g2bajd.zfba_xs_jcqbhsjdsxx t2
               left join g2bajd.zfba_xs_qbhsjdsxx qb on qb.FZXYRBH = t2.fzxyrbh
      where t2.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
        and t2.jlbz = '1') t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
where 1 = 1
  -- #if :sjjgdm != '9999'
  and gx.SJJGDM = :sjjgdm
  -- #fi
  -- #switch :lx
  -- #case 'zs'
  and t.lx = '1'
  -- #break
  -- #case 'rbs'
  and t.lx = '1'
  and t.qbfs = '1'
  -- #break
  -- #case 'cbs'
  and t.lx = '1'
  and t.qbfs = '2'
  -- #break
  -- #case 'jczs'
  and t.lx = '2'
  -- #break
  -- #case 'jcrbs'
  and t.lx = '2'
  and t.qbfs = '1'
  -- #break
  -- #case 'jccbs'
  and t.lx = '2'
  and t.qbfs = '2'
  -- #break
  -- #case 'qbhxj'
  and exists(select 1
             from g2bajd.zfba_xs_jlzxx jl
             where jl.lrsj > t.lrsj
               and jl.jlbz = '1'
               and t.lx = '1'
               and jl.ajywh = t.ajywh
               and jl.fzxyrbh = t.fzxyrbh)
  -- #break
  -- #case 'qbhjj'
  and exists(select 1
             from g2bajd.zfba_xs_jsjzjdsxx jj
             where jj.lrsj > t.lrsj
               and jj.jlbz = '1'
               and t.lx = '1'
               and jj.ajywh = t.ajywh
               and jj.fzxyrbh = t.fzxyrbh)
-- #break
  -- #case 'qbhdb'
  and exists(select 1
             from g2bajd.zfba_xs_dbzxx db
             where db.lrsj > t.lrsj
               and db.jlbz = '1'
               and t.lx = '1'
               and db.ajywh = t.ajywh
               and db.fzxyrbh = t.fzxyrbh)
-- #break
-- #case 'qbhqs'
  and exists(select 1
             from g2bajd.zfba_xs_qsyjsxx qs
             where qs.lrsj > t.lrsj
               and qs.jlbz = '1'
               and t.lx = '1'
               and qs.ajywh = t.ajywh
               and qs.cldxbh like '%' || t.fzxyrbh || '%')
-- #break
-- #case 'qbhwqs'
  and not exists(select 1
                 from g2bajd.zfba_xs_qsyjsxx qs
                 where qs.lrsj > t.lrsj
                   and qs.jlbz = '1'
                   and t.lx = '1'
                   and qs.ajywh = t.ajywh
                   and qs.cldxbh like '%' || t.fzxyrbh || '%')
-- #break
-- #end
  and gx.dm like :dwdm || '%'
order by lrsj desc
;

/*[qzcstj-db]*/
/*#强制措施逮捕统计数据#*/
select *
from (
-- #if :dwjb = 40
         select gx.sjjgdm,
                gx.sjjgmc,
                substr(gx.sjjgdm, 1, 6) as                                                          dwdm,
                '合计' as dwmc,
                gx.px,
                count(t.dbzbh)                                                                      zs,
                count(case when t.SFSSZX = '1' then t.dbzbh end)                                    zxhzs,
                count(case
                          when exists(select 1
                                      from g2kss.kss_aryrs r
                                      where r.ajbh = t.ajbh
                                        and r.xyrbh = t.fzxyrbh
                                        and r.jlbz = '1'
                                        and r.rsyy = '12'
                                        and r.C_CZSJ > t.lrsj) then t.dbzbh end)                    rksss,
                count(case
                          when t.SFSSZX = '0' and exists(select 1
                                                         from g2bajd.zfba_gt_xyrztxx zt
                                                         where zt.WFFZXYRBH = t.fzxyrbh
                                                           and zt.jlbz = '1') then t.dbzbh end)     dbzts,
                count(case
                          when t.SFSSZX = '0' and not exists(select 1
                                                             from g2bajd.zfba_gt_xyrztxx zt
                                                             where zt.WFFZXYRBH = t.fzxyrbh
                                                               and zt.jlbz = '1') then t.dbzbh end) qtwzxs,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_qbhsjdsxx qb
                                      where qb.lrsj > t.lrsj
                                        and qb.jlbz = '1'
                                        and qb.ajywh = t.ajywh
                                        and qb.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhqb,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_jsjzjdsxx jj
                                      where jj.lrsj > t.lrsj
                                        and jj.jlbz = '1'
                                        and jj.ajywh = t.ajywh
                                        and jj.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhjj,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_sftzsxx sf
                                      where sf.lrsj > t.lrsj
                                        and sf.jlbz = '1'
                                        and sf.ajywh = t.ajywh
                                        and sf.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhsf,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_ycjyqxyjsxx yc
                                      where yc.lrsj > t.lrsj
                                        and yc.jlbz = '1'
                                        and yc.ajywh = t.ajywh
                                        and yc.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhyc,
                count(case
                          when not exists(select 1
                                          from g2bajd.zfba_xs_qsyjsxx qs
                                          where qs.lrsj > t.lrsj
                                            and qs.jlbz = '1'
                                            and qs.ajywh = t.ajywh
                                            and qs.cldxbh like '%' || t.fzxyrbh || '%')
                              then t.dbzbh end)                                                     dbhwqs,
                count(case
                          when exists(select 1
                                      from g2bajd.ZFBA_GT_CQBGS b
                                      where wslx = '3093'
                                        and b.jlbz = '1'
                                        and b.lrsj > t.lrsj
                                        and b.ajywh = t.ajywh)
                              then t.dbzbh end)                                                     dbhzlxz
         from g2bajd.zfba_xs_dbzxx t
                  join (select *
                        from zhag_sys_jggx
                        where sjjgjz = '38'
                          and sjjgjb <> '40'
                          and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                       on gx.dm = t.lrdwdm
         where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
           and t.jlbz = '1'
           -- #if :dwjb != 40
           and gx.SJJGDM = :sjjgdm
           -- #fi
           and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
         group by gx.sjjgdm, gx.sjjgmc, gx.px
         union all
-- #fi
         select '9999' as                       sjjgdm,
                '总数' as                       sjjgmc,
                '5301' as                       dwdm,
                ''     as                       dwmc,
                '9999' as                       px,
                count(t.dbzbh)                                                                      zs,
                count(case when t.SFSSZX = '1' then t.dbzbh end)                                    zxhzs,
                count(case
                          when exists(select 1
                                      from g2kss.kss_aryrs r
                                      where r.ajbh = t.ajbh
                                        and r.xyrbh = t.fzxyrbh
                                        and r.jlbz = '1'
                                        and r.rsyy = '12'
                                        and r.C_CZSJ > t.lrsj) then t.dbzbh end)                    rksss,
                count(case
                          when t.SFSSZX = '0' and exists(select 1
                                                         from g2bajd.zfba_gt_xyrztxx zt
                                                         where zt.WFFZXYRBH = t.fzxyrbh
                                                           and zt.jlbz = '1') then t.dbzbh end)     dbzts,
                count(case
                          when t.SFSSZX = '0' and not exists(select 1
                                                             from g2bajd.zfba_gt_xyrztxx zt
                                                             where zt.WFFZXYRBH = t.fzxyrbh
                                                               and zt.jlbz = '1') then t.dbzbh end) qtwzxs,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_qbhsjdsxx qb
                                      where qb.lrsj > t.lrsj
                                        and qb.jlbz = '1'
                                        and qb.ajywh = t.ajywh
                                        and qb.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhqb,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_jsjzjdsxx jj
                                      where jj.lrsj > t.lrsj
                                        and jj.jlbz = '1'
                                        and jj.ajywh = t.ajywh
                                        and jj.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhjj,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_sftzsxx sf
                                      where sf.lrsj > t.lrsj
                                        and sf.jlbz = '1'
                                        and sf.ajywh = t.ajywh
                                        and sf.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhsf,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_ycjyqxyjsxx yc
                                      where yc.lrsj > t.lrsj
                                        and yc.jlbz = '1'
                                        and yc.ajywh = t.ajywh
                                        and yc.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhyc,
                count(case
                          when not exists(select 1
                                          from g2bajd.zfba_xs_qsyjsxx qs
                                          where qs.lrsj > t.lrsj
                                            and qs.jlbz = '1'
                                            and qs.ajywh = t.ajywh
                                            and qs.cldxbh like '%' || t.fzxyrbh || '%')
                              then t.dbzbh end)                                                     dbhwqs,
                count(case
                          when exists(select 1
                                      from g2bajd.ZFBA_GT_CQBGS b
                                      where wslx = '3093'
                                        and b.jlbz = '1'
                                        and b.lrsj > t.lrsj
                                        and b.ajywh = t.ajywh)
                              then t.dbzbh end)                                                     dbhzlxz
         from g2bajd.zfba_xs_dbzxx t
                  join (select *
                        from zhag_sys_jggx
                        where sjjgjz = '38'
                          and sjjgjb <> '40'
                          and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                       on gx.dm = t.lrdwdm
         where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
           and t.jlbz = '1'
           and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
         union all
         select gx.sjjgdm,
                gx.sjjgmc,
                t.lrdwdm,
                nvl(jg.jc, jg.mc) as                                                                jjdwmc,
                gx.px,
                count(t.dbzbh)                                                                      zs,
                count(case when t.SFSSZX = '1' then t.dbzbh end)                                    zxhzs,
                count(case
                          when exists(select 1
                                      from g2kss.kss_aryrs r
                                      where r.ajbh = t.ajbh
                                        and r.xyrbh = t.fzxyrbh
                                        and r.jlbz = '1'
                                        and r.rsyy = '12'
                                        and r.C_CZSJ > t.lrsj) then t.dbzbh end)                    rksss,
                count(case
                          when t.SFSSZX = '0' and exists(select 1
                                                         from g2bajd.zfba_gt_xyrztxx zt
                                                         where zt.WFFZXYRBH = t.fzxyrbh
                                                           and zt.jlbz = '1') then t.dbzbh end)     dbzts,
                count(case
                          when t.SFSSZX = '0' and not exists(select 1
                                                             from g2bajd.zfba_gt_xyrztxx zt
                                                             where zt.WFFZXYRBH = t.fzxyrbh
                                                               and zt.jlbz = '1') then t.dbzbh end) qtwzxs,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_qbhsjdsxx qb
                                      where qb.lrsj > t.lrsj
                                        and qb.jlbz = '1'
                                        and qb.ajywh = t.ajywh
                                        and qb.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhqb,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_jsjzjdsxx jj
                                      where jj.lrsj > t.lrsj
                                        and jj.jlbz = '1'
                                        and jj.ajywh = t.ajywh
                                        and jj.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhjj,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_sftzsxx sf
                                      where sf.lrsj > t.lrsj
                                        and sf.jlbz = '1'
                                        and sf.ajywh = t.ajywh
                                        and sf.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhsf,
                count(case
                          when exists(select 1
                                      from g2bajd.zfba_xs_ycjyqxyjsxx yc
                                      where yc.lrsj > t.lrsj
                                        and yc.jlbz = '1'
                                        and yc.ajywh = t.ajywh
                                        and yc.fzxyrbh = t.fzxyrbh)
                              then t.dbzbh end)                                                     dbhyc,
                count(case
                          when not exists(select 1
                                          from g2bajd.zfba_xs_qsyjsxx qs
                                          where qs.lrsj > t.lrsj
                                            and qs.jlbz = '1'
                                            and qs.ajywh = t.ajywh
                                            and qs.cldxbh like '%' || t.fzxyrbh || '%')
                              then t.dbzbh end)                                                     dbhwqs,
                count(case
                          when exists(select 1
                                      from g2bajd.ZFBA_GT_CQBGS b
                                      where wslx = '3093'
                                        and b.jlbz = '1'
                                        and b.lrsj > t.lrsj
                                        and b.ajywh = t.ajywh)
                              then t.dbzbh end)                                                     dbhzlxz
         from g2bajd.zfba_xs_dbzxx t
                  join (select *
                        from zhag_sys_jggx
                        where sjjgjz = '38'
                          and sjjgjb <> '40'
                          and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                       on gx.dm = t.lrdwdm
                  left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
         where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
           and t.jlbz = '1'
           -- #if :dwjb != 40
           and gx.SJJGDM = :sjjgdm
           -- #fi
           and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
         group by gx.sjjgdm, gx.sjjgmc, gx.px, t.lrdwdm, nvl(jg.jc, jg.mc))
order by px, dwdm
;

/*[qzcstj-dbdetail]*/
/*#强制措施逮捕统计详情数据#*/
select t.*
from g2bajd.zfba_xs_dbzxx t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  and t.jlbz = '1'
  -- #if :sjjgdm != '9999'
  and gx.SJJGDM = :sjjgdm
  -- #fi
  -- #switch :lx
  -- #case 'zs'
  and 1 = 1
  -- #break
  -- #case 'zxhzs'
  and t.SFSSZX = '1'
  -- #break
  -- #case 'rksss'
  and exists(select 1
             from g2kss.kss_aryrs r
             where r.ajbh = t.ajbh
               and r.xyrbh = t.fzxyrbh
               and r.jlbz = '1'
               and r.rsyy = '12'
               and r.C_CZSJ > t.lrsj)
  -- #break
  -- #case 'dbzts'
  and t.SFSSZX = '0'
  and exists(select 1
             from g2bajd.zfba_gt_xyrztxx zt
             where zt.WFFZXYRBH = t.fzxyrbh
               and zt.jlbz = '1')
  -- #break
  -- #case 'qtwzxs'
  and t.SFSSZX = '0'
  and not exists(select 1
                 from g2bajd.zfba_gt_xyrztxx zt
                 where zt.WFFZXYRBH = t.fzxyrbh
                   and zt.jlbz = '1')
  -- #break
  -- #case 'dbhqb'
  and exists(select 1
             from g2bajd.zfba_xs_qbhsjdsxx qb
             where qb.lrsj > t.lrsj
               and qb.jlbz = '1'
               and qb.ajywh = t.ajywh
               and qb.fzxyrbh = t.fzxyrbh)
-- #break
  -- #case 'dbhjj'
  and exists(select 1
             from g2bajd.zfba_xs_jsjzjdsxx jj
             where jj.lrsj > t.lrsj
               and jj.jlbz = '1'
               and jj.ajywh = t.ajywh
               and jj.fzxyrbh = t.fzxyrbh)
-- #break
  -- #case 'dbhsf'
  and exists(select 1
             from g2bajd.zfba_xs_sftzsxx sf
             where sf.lrsj > t.lrsj
               and sf.jlbz = '1'
               and sf.ajywh = t.ajywh
               and sf.fzxyrbh = t.fzxyrbh)
  -- #break
  -- #case 'dbhzlxz'
  and exists(select 1
             from g2bajd.ZFBA_GT_CQBGS b
             where wslx = '3093'
               and b.jlbz = '1'
               and b.lrsj > t.lrsj
               and b.ajywh = t.ajywh)
-- #break
-- #case 'dbhyc'
  and exists(select 1
             from g2bajd.zfba_xs_ycjyqxyjsxx yc
             where yc.lrsj > t.lrsj
               and yc.jlbz = '1'
               and yc.ajywh = t.ajywh
               and yc.fzxyrbh = t.fzxyrbh)
-- #break
-- #case 'dbhwqs'
  and not exists(select 1
                 from g2bajd.zfba_xs_qsyjsxx qs
                 where qs.lrsj > t.lrsj
                   and qs.jlbz = '1'
                   and qs.ajywh = t.ajywh
                   and qs.cldxbh like '%' || t.fzxyrbh || '%')
-- #break
-- #end
  and gx.dm like :dwdm || '%'
order by t.lrsj desc;

/*[zccstj]*/
/*#侦查措施统计数据#*/
select *
from (
-- #if :dwjb = 40
         select gx.sjjgdm,
                gx.sjjgmc,
                substr(gx.sjjgdm, 1, 6) as                     dwdm,
                '合计' as dwmc,
                gx.px,
                count(case when t.lxdm = '2004' then t.wh end) xws,
                count(case when t.lxdm = '3080' then t.wh end) xws2,
                count(case when t.lxdm = '2024' then t.wh end) kyjcs,
                count(case when t.lxdm = '3037' then t.wh end) scs,
                count(case when t.lxdm = '3128' then t.wh end) cfs,
                count(case when t.lxdm = '3108' then t.wh end) kys,
                count(case when t.lxdm = '3046' then t.wh end) cxs,
                count(case when t.lxdm = '3047' then t.wh end) djs,
                count(case when t.lxdm = '3087' then t.wh end) jds,
                count(case when t.lxdm = '2069' then t.wh end) brs
         from zhag.v_zhag_xsajzccs t
                  join (select *
                        from zhag_sys_jggx
                        where sjjgjz = '38'
                          and sjjgjb <> '40'
                          and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                       on gx.dm = t.lrdwdm
         where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
           -- #if :dwjb != '40'
           and gx.SJJGDM = :sjjgdm
           -- #fi
           and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
         group by gx.sjjgdm, gx.sjjgmc, gx.px
         union all
-- #fi
         select '9999' as                                      sjjgdm,
                '总数' as                                      sjjgmc,
                '5301' as                                      dwdm,
                ''     as                                      dwmc,
                '9999' as                                      px,
                count(case when t.lxdm = '2004' then t.wh end) xws,
                count(case when t.lxdm = '3080' then t.wh end) xws2,
                count(case when t.lxdm = '2024' then t.wh end) kyjcs,
                count(case when t.lxdm = '3037' then t.wh end) scs,
                count(case when t.lxdm = '3128' then t.wh end) cfs,
                count(case when t.lxdm = '3108' then t.wh end) kys,
                count(case when t.lxdm = '3046' then t.wh end) cxs,
                count(case when t.lxdm = '3047' then t.wh end) djs,
                count(case when t.lxdm = '3087' then t.wh end) jds,
                count(case when t.lxdm = '2069' then t.wh end) brs
         from zhag.v_zhag_xsajzccs t
                  join (select *
                        from zhag_sys_jggx
                        where sjjgjz = '38'
                          and sjjgjb <> '40'
                          and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                       on gx.dm = t.lrdwdm
         where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
           -- #if :dwjb != '40'
           and gx.SJJGDM = :sjjgdm
           -- #fi
           and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
         union all
         select gx.sjjgdm,
                gx.sjjgmc,
                t.lrdwdm,
                nvl(jg.jc, jg.mc) as                           lrdwmc,
                gx.px,
                count(case when t.lxdm = '2004' then t.wh end) xws,
                count(case when t.lxdm = '3080' then t.wh end) xws2,
                count(case when t.lxdm = '2024' then t.wh end) kyjcs,
                count(case when t.lxdm = '3037' then t.wh end) scs,
                count(case when t.lxdm = '3128' then t.wh end) cfs,
                count(case when t.lxdm = '3108' then t.wh end) kys,
                count(case when t.lxdm = '3046' then t.wh end) cxs,
                count(case when t.lxdm = '3047' then t.wh end) djs,
                count(case when t.lxdm = '3087' then t.wh end) jds,
                count(case when t.lxdm = '2069' then t.wh end) brs
         from zhag.v_zhag_xsajzccs t
                  join (select *
                        from zhag_sys_jggx
                        where sjjgjz = '38'
                          and sjjgjb <> '40'
                          and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                       on gx.dm = t.lrdwdm
                  left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
         where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
-- #if :dwjb != 40
           and gx.SJJGDM = :sjjgdm
-- #fi
           and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
         group by gx.sjjgdm, gx.sjjgmc, gx.px, t.lrdwdm, nvl(jg.jc, jg.mc))
order by px, dwdm;

/*[zccstj-detail]*/
/*#侦查措施统计详情数据#*/
select *
from zhag.v_zhag_xsajzccs t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
-- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and lxdm = :lxdm
  and gx.dm like :dwdm || '%'
order by t.lrsj desc;

/*[xsflwstj]*/
/*#刑事案件法律文书统计数据#*/
select t.wslx, t.wsmc, count(1) as sl
from g2bajd.zfba_gt_flws t
         join (select *
               from zhag.zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
where t.ajlx <> '11'
  and t.sfscws = '1'
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
-- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
-- #if :wslx != blank
  and t.wsmc like '%'||:wslx||'%'
-- #fi
group by t.wslx, t.wsmc
order by wslx;

/*[sshjtj-sla]*/
/*#刑事案件诉讼环节受立案统计#*/
select *
from (select gx.sjjgdm,
             gx.sjjgmc,
             substr(gx.sjjgdm, 1, 6) as                                                   dwdm,
             '合计'                  as                                                   dwmc,
             gx.px,
             count(case when lx = '1' then t.cqbh end)                                    cqsa,
             count(case when lx = '1' and t.spjg = '02' then t.cqbh end)                  btysa,
             count(case when lx = '1' and t.spjg = '01' then t.cqbh end)                  tysa,
             count(case
                       when lx = '1' and exists (select 1 from g2bajd.zfba_gt_sahz hz where hz.ajywh = t.ajywh)
                           then t.ajbh end)                                               sahz,
             count(case
                       when lx = '1' and  exists (select 1 from g2bajd.zfba_gt_ajjbxx aj where aj.ajywh = t.ajywh and aj.ajzt='14')
                           then t.ajbh end)                                               ysawla,
             count(case when lx = '2' then t.cqbh end)                                    cqla,
             count(case when lx = '2' and t.spjg = '04' then t.cqbh end)                  labth,
             count(case when lx = '2' and t.spjg = '01' then t.cqbh end)                  tyla,
             count(case
                       when lx = '2' and
                            exists (select 1 from g2bajd.zfba_gt_gzs gz where gz.gzslx = '01' and gz.ajywh = t.ajywh)
                           then t.ajbh end)                                               lagz,
             count(case when lx = '3' then t.ajbh end)                                    ysaj,
             count(case when lx = '4' then t.ajbh end)                                    cxaj
             /*count(case when lx = '5' then t.ajbh end)                                    lajd,
             count(case
                       when lx = '5' and exists(select 1
                                                from g2bajd.ZFBA_ZFGX_HKXX x
                                                         join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
                                                where x.ywlb = 'CO380-R08'
                                                  and t.spjg = d.sjywid) then t.ajbh end) lajdjzwf*/
      from (select spbh as cqbh, ajbh, ajywh, spclsj as lrsj, spdwdm as lrdwdm, spdwmc as lrdwmc, spjg, '1' as lx
            from g2bajd.zfba_gt_ajslsp
            where jlbz = '1'
              and spclsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            union all
            select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, cqzt, '2'
            from g2bajd.zfba_gt_cqbgs
            where wslx = '3066'
              and jlbz = '1'
              and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            union all
            select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, '', '3'
            from g2bajd.zfba_xs_ysajtzs
            where jlbz = '1'
              and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            union all
            select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, '', '4'
            from g2bajd.zfba_xs_cxajjdsxx
            where jlbz = '1'
              and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            /*union all
            select d.ajbh, '', x.rksj, d.xtdwdm, d.xtdwmc, d.sjywid, '5'
            from g2bajd.ZFBA_ZFGX_HKXX x
                     left join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
            where x.ywlb = 'CO380-R01'
              and x.rksj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and x.jlbz = '1'*/
            ) t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
-- #if :sjjgdm != '9999'
                        and gx.SJJGDM = :sjjgdm
                        -- #fi
                        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      group by gx.sjjgdm, gx.sjjgmc, gx.px
      union all
      select '9999',
             '总数',
             '5301',
             '',
             '9999',
             count(case when lx = '1' then t.cqbh end)                                    cqsa,
             count(case when lx = '1' and t.spjg = '02' then t.cqbh end)                  btysa,
             count(case when lx = '1' and t.spjg = '01' then t.cqbh end)                  tysa,
             count(case
                       when lx = '1' and exists (select 1 from g2bajd.zfba_gt_sahz hz where hz.ajywh = t.ajywh)
                           then t.ajbh end)                                               sahz,
             count(case
                       when lx = '1' and  exists (select 1 from g2bajd.zfba_gt_ajjbxx aj where aj.ajywh = t.ajywh and aj.ajzt='14')
                           then t.ajbh end)                                               ysawla,
             count(case when lx = '2' then t.cqbh end)                                    cqla,
             count(case when lx = '2' and t.spjg = '04' then t.cqbh end)                  labth,
             count(case when lx = '2' and t.spjg = '01' then t.cqbh end)                  tyla,
             count(case
                       when lx = '2' and
                            exists (select 1 from g2bajd.zfba_gt_gzs gz where gz.gzslx = '01' and gz.ajywh = t.ajywh)
                           then t.ajbh end)                                               lagz,
             count(case when lx = '3' then t.ajbh end)                                    ysaj,
             count(case when lx = '4' then t.ajbh end)                                    cxaj
             /*count(case when lx = '5' then t.ajbh end)                                    lajd,
             count(case
                       when lx = '5' and exists(select 1
                                                from g2bajd.ZFBA_ZFGX_HKXX x
                                                         join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
                                                where x.ywlb = 'CO380-R08'
                                                  and t.spjg = d.sjywid) then t.ajbh end) lajdjzwf*/
      from (select spbh as cqbh, ajbh, ajywh, spclsj as lrsj, spdwdm as lrdwdm, spdwmc as lrdwmc, spjg, '1' as lx
            from g2bajd.zfba_gt_ajslsp
            where jlbz = '1'
              and spclsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            union all
            select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, cqzt, '2'
            from g2bajd.zfba_gt_cqbgs
            where wslx = '3066'
              and jlbz = '1'
              and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            union all
            select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, '', '3'
            from g2bajd.zfba_xs_ysajtzs
            where jlbz = '1'
              and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            union all
            select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, '', '4'
            from g2bajd.zfba_xs_cxajjdsxx
            where jlbz = '1'
              and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            /*union all
            select d.ajbh, '', x.rksj, d.xtdwdm, d.xtdwmc, d.sjywid, '5'
            from g2bajd.ZFBA_ZFGX_HKXX x
                     left join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
            where x.ywlb = 'CO380-R01'
              and x.rksj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and x.jlbz = '1'*/
              ) t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
      where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
        -- #if :dwjb != '9999'
        and gx.SJJGDM = :sjjgdm
        -- #fi
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
-- #if :dwjb = 40
      union all
      select gx.sjjgdm,
             gx.sjjgmc,
             t.lrdwdm,
             nvl(jg.jc, jg.mc) as                                                         lrdwmc,
             gx.px,
             count(case when lx = '1' then t.cqbh end)                                    cqsa,
             count(case when lx = '1' and t.spjg = '02' then t.cqbh end)                  btysa,
             count(case when lx = '1' and t.spjg = '01' then t.cqbh end)                  tysa,
             count(case
                       when lx = '1' and exists (select 1 from g2bajd.zfba_gt_sahz hz where hz.ajywh = t.ajywh)
                           then t.ajbh end)                                               sahz,
             count(case
                       when lx = '1' and  exists (select 1 from g2bajd.zfba_gt_ajjbxx aj where aj.ajywh = t.ajywh and aj.ajzt='14')
                           then t.ajbh end)                                               ysawla,
             count(case when lx = '2' then t.cqbh end)                                    cqla,
             count(case when lx = '2' and t.spjg = '04' then t.cqbh end)                  labth,
             count(case when lx = '2' and t.spjg = '01' then t.cqbh end)                  tyla,
             count(case
                       when lx = '2' and
                            exists (select 1 from g2bajd.zfba_gt_gzs gz where gz.gzslx = '01' and gz.ajywh = t.ajywh)
                           then t.ajbh end)                                               lagz,
             count(case when lx = '3' then t.ajbh end)                                    ysaj,
             count(case when lx = '4' then t.ajbh end)                                    cxaj
             /*count(case when lx = '5' then t.ajbh end)                                    lajd,
             count(case
                       when lx = '5' and exists(select 1
                                                from g2bajd.ZFBA_ZFGX_HKXX x
                                                         join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
                                                where x.ywlb = 'CO380-R08'
                                                  and t.spjg = d.sjywid) then t.ajbh end) lajdjzwf*/
      from (select spbh as cqbh, ajbh, ajywh, spclsj as lrsj, spdwdm as lrdwdm, spdwmc as lrdwmc, spjg, '1' as lx
            from g2bajd.zfba_gt_ajslsp
            where jlbz = '1'
              and spclsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            union all
            select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, cqzt, '2'
            from g2bajd.zfba_gt_cqbgs
            where wslx = '3066'
              and jlbz = '1'
              and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            union all
            select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, '', '3'
            from g2bajd.zfba_xs_ysajtzs
            where jlbz = '1'
              and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            union all
            select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, '', '4'
            from g2bajd.zfba_xs_cxajjdsxx
            where jlbz = '1'
              and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
            /*union all
            select d.ajbh, '', x.rksj, d.xtdwdm, d.xtdwmc, d.sjywid, '5'
            from g2bajd.ZFBA_ZFGX_HKXX x
                     left join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
            where x.ywlb = 'CO380-R01'
              and x.rksj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
              and x.jlbz = '1'*/
              ) t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.lrdwdm
               left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
      where t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
-- #if :dwjb != 40
        and gx.SJJGDM = :sjjgdm
-- #fi
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
      group by gx.sjjgdm, gx.sjjgmc, gx.px, t.lrdwdm, nvl(jg.jc, jg.mc)
-- #fi
     )
order by px, dwdm
;

/*[sshjtj-sladetxail]*/
/*#刑事案件诉讼环节受立案统计详情数据#*/
select *
from (select spbh as cqbh, ajbh, ajywh, spclsj as lrsj, spdwdm as lrdwdm, spdwmc as lrdwmc, spjg, '1' as lx
      from g2bajd.zfba_gt_ajslsp
      where jlbz = '1'
        and spclsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
      union all
      select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, cqzt, '2'
      from g2bajd.zfba_gt_cqbgs
      where wslx = '3066'
        and jlbz = '1'
        and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
      union all
      select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, '', '3'
      from g2bajd.zfba_xs_ysajtzs
      where jlbz = '1'
        and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
      union all
      select cqbh, ajbh, ajywh, lrsj, lrdwdm, lrdwmc, '', '4'
      from g2bajd.zfba_xs_cxajjdsxx
      where jlbz = '1'
        and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
      /*union all
      select d.ajbh, '', x.rksj, d.xtdwdm, d.xtdwmc, d.sjywid, '5'
      from g2bajd.ZFBA_ZFGX_HKXX x
               join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
      where x.ywlb = 'CO380-R01'
        and x.rksj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
        and x.jlbz = '1'*/
        ) t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
where 1=1
-- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
-- #switch :lx
  -- #case 'cqsa'
  and lx = '1'
  -- #break
  -- #case 'btysa'
  and lx = '1'
  and spjg = '02'
  -- #break
  -- #case 'tysa'
  and lx = '1'
  and spjg = '01'
  -- #break
  -- #case 'sahz'
  and lx = '1'
  and exists (select 1 from g2bajd.zfba_gt_sahz hz where hz.ajywh = t.ajywh)
  -- #break
  -- #case 'ysawla'
  and lx = '1'
  and exists (select 1 from g2bajd.zfba_gt_ajjbxx aj where aj.ajywh = t.ajywh and aj.ajzt='14')
  -- #break
-- #case 'cqla'
  and lx = '2'
  -- #break
  -- #case 'labth'
  and lx = '2'
  and spjg = '04'
  -- #break
  -- #case 'tyla'
  and lx = '2'
  and spjg = '01'
  -- #break
  -- #case 'lagz'
  and lx = '1'
  and exists (select 1 from g2bajd.zfba_gt_gzs gz where gz.gzslx = '01' and gz.ajywh = t.ajywh)
  -- #break
-- #case 'ysaj'
  and lx = '3'
  -- #break
  -- #case 'cxla'
  and lx = '4'
  -- #break
  -- #case 'lajd'
  and lx = '5'
  -- #break
  -- #case 'lajdjzwf'
  and lx = '5'
  and exists(select 1
             from g2bajd.ZFBA_ZFGX_HKXX x
                      join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
             where x.ywlb = 'CO380-R08'
               and t.spjg = d.sjywid)
  -- #break
-- #end
  and gx.dm like :dwdm || '%'
order by t.lrsj desc;

/*[sshjtj-zc]*/
/*#侦查环节节点数据统计#*/
with a as (select gx.sjjgdm,
                  gx.sjjgmc,
                  t.lrdwdm                                                                          as dwdm,
                  nvl(jg.jc, jg.mc)                                                                 as lrdwmc,
                  gx.px,
                  count(distinct t.rwid)                                                               zs,
                  count(distinct (case when cqzt = '00' then t.rwid end))                           as spz,
                  count(distinct (case when cqzt in ('01', '02') then t.rwid end))                  as spwc,
                  count(distinct (case when d.jlbh is not null then t.ajbh end))                    as pzjdajs,
                  count(distinct (case when d.jlbh is not null then r.zjhm end))                    as pzjdrs,
                  count(distinct (case when r.ryzt = '02' then t.ajbh end))                         as pzajs,
                  count(distinct (case when r.ryzt = '02' then r.zjhm end))                         as pzrs,
                  count(distinct (case when r.ryzt = '03' and zd.ywfl = 'xjsf002' then r.zjhm end)) as jdbb,
                  count(distinct (case when r.ryzt = '03' and zd.ywfl = 'xjsf003' then r.zjhm end)) as cybb,
                  count(distinct (case when r.ryzt = '03' and zd.ywfl = 'xjsf001' then r.zjhm end)) as xdbb,
                  count(distinct (case
                                      when exists(select 1
                                                  from g2bajd.zfba_xs_sftzsxx sf
                                                           join g2bajd.zfba_gt_wffzxyr xyr on sf.fzxyrbh = xyr.wffzxyrbh
                                                  where t.ajywh = sf.ajywh
                                                    and xyr.zjhm = r.zjhm) then r.zjhm end))        as sf,
                  count(distinct (case
                                      when not EXISTS(select 1
                                                      from g2bajd.ZFBA_ZFGX_XTXX t2
                                                      where t2.rodm = 'CO100-R04'
                                                        and t2.rwid = t.rwid)
                                          then t.ajbh end))                                         as dbwhz
           from g2bajd.ZFBA_ZFGX_XTXX t
                    left join g2bajd.ZFBA_ZFGX_DBPZXX d on t.rwid = d.sjrwid
                    left join g2bajd.ZFBA_ZFGX_DBPZXX_XYRXX r on d.jlbh = r.dbjlbh
                    left join zhag.zhag_dicvalue zd on zd.zdlb = '050901' and zd.dm = r.JDYYDM
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = t.lrdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where t.jlbz = '1'
             and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             and rodm = 'CO100-R01'
             and tqpzdbsbh is not null
-- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
           group by gx.sjjgdm, gx.sjjgmc, gx.px, t.LRDWDM, nvl(jg.jc, jg.mc)),
     b as (select gx.sjjgdm,
                  gx.sjjgmc,
                  t.lrdwdm                                                    as dwdm,
                  nvl(jg.jc, jg.mc)                                           as lrdwmc,
                  gx.px,
                  count(1)                                                       zs,
                  count(case when cqzt in ('00', '03', '05') then t.cqbh end) as spz,
                  count(case when cqzt in ('01', '02') then t.cqbh end)       as spwc,
                  count(case when cqzt = '04' then t.cqbh end)                as spth
           from g2bajd.ZFBA_GT_CQBGS t
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = t.lrdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where t.jlbz = '1'
             and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             and wslx = '3028'
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
           group by gx.sjjgdm, gx.sjjgmc, gx.px, t.LRDWDM, nvl(jg.jc, jg.mc))
select *
from (select nvl(a.sjjgdm, b.sjjgdm) as sjjgdm,
             nvl(a.sjjgmc, b.sjjgmc) as sjjgmc,
             nvl(a.dwdm, b.dwdm)     as dwdm,
             nvl(a.lrdwmc, a.lrdwmc) as dwmc,
             nvl(a.px, b.px)         as px,
             nvl(b.zs, 0)            as cqzs,
             nvl(b.spz, 0)           as cqspz,
             nvl(b.spwc, 0)          as cqspwc,
             nvl(b.spth, 0)          as cqspth,
             nvl(a.zs, 0)            as xtzs,
             nvl(a.spz, 0)           as xzspz,
             nvl(a.spwc, 0)          as xzspwc,
             nvl(a.pzjdajs, 0)       as xtpzjdajs,
             nvl(a.pzjdrs, 0)        as xtpzjdrs,
             nvl(a.pzajs, 0)         as xtpzajs,
             nvl(a.pzrs, 0)          as xtpzrs,
             nvl(a.jdbb, 0)          as xtjdbb,
             nvl(a.cybb, 0)          as xtcybb,
             nvl(a.xdbb, 0)          as xtxdbb,
             nvl(a.sf, 0)            as xtsf,
             nvl(a.dbwhz, 0)         as xtdbwhz
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
      union all
      select '9999',
             '总数',
             '5301',
             '',
             '9999',
             sum(b.zs),
             sum(b.spz),
             sum(b.spwc),
             sum(b.spth),
             sum(a.zs),
             sum(a.spz),
             sum(a.spwc),
             sum(pzjdajs),
             sum(pzjdrs),
             sum(pzajs),
             sum(pzrs),
             sum(jdbb),
             sum(cybb),
             sum(xdbb),
             sum(sf),
             sum(dbwhz)
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
-- #if :dwjb = 40
      union all
      select nvl(a.sjjgdm, b.sjjgdm),
             nvl(a.sjjgmc, b.sjjgmc),
             substr(nvl(a.sjjgdm, b.sjjgdm), 1, 6) as dwdm,
             '合计',
             nvl(a.px, b.px),
             sum(b.zs),
             sum(b.spz),
             sum(b.spwc),
             sum(b.spth),
             sum(a.zs),
             sum(a.spz),
             sum(a.spwc),
             sum(pzjdajs),
             sum(pzjdrs),
             sum(pzajs),
             sum(pzrs),
             sum(jdbb),
             sum(cybb),
             sum(xdbb),
             sum(sf),
             sum(dbwhz)
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
-- #if :sjjgdm != '9999'
        and nvl(a.sjjgdm, b.sjjgdm) = :sjjgdm
      -- #fi
      group by nvl(a.sjjgdm, b.sjjgdm), nvl(a.sjjgmc, b.sjjgmc), nvl(a.px, b.px)
-- #fi
     )
order by px, dwdm
;
/*[sshjtj-zcdetailaj]*/
/*#呈请提请批准逮捕报告书数据#*/
select *
from g2bajd.ZFBA_GT_CQBGS t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where t.jlbz = '1'
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  and wslx = '3028'
-- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and gx.dm like :dwdm || '%'
-- #switch :lx
  -- #case 'cqzs'
  and 1 = '1'
-- #break
  -- #case 'cqspz'
  and cqzt in ('00', '03', '05')
-- #break
  -- #case 'cqspwc'
  and cqzt in ('01', '02')
-- #break
  -- #case 'cqspth'
  and cqzt = '04'
-- #break
--#end
order by lrsj desc;

/*[sshjtj-zcxtdetailaj]*/
/*#发起提捕协同流程案件数据#*/
select t.rwmc,
       t.ajbh,
       t.ajywh,
       t.fqdwdm,
       t.fqdwmc,
       t.xtdwmc,
       t.lrsj
from g2bajd.ZFBA_ZFGX_XTXX t
         left join g2bajd.ZFBA_ZFGX_DBPZXX d on t.rwid = d.sjrwid
         left join g2bajd.ZFBA_ZFGX_DBPZXX_XYRXX r on d.jlbh = r.dbjlbh
         left join zhag.zhag_dicvalue zd on zd.zdlb = '050901' and zd.dm = r.JDYYDM
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where t.jlbz = '1'
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  and rodm = 'CO100-R01'
  and tqpzdbsbh is not null
-- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and gx.dm like :dwdm || '%'
-- #switch :lx
  -- #case 'xtzs'
  and 1 = '1'
  -- #break
  -- #case 'xzspz'
  and cqzt = '00'
  -- #break
  -- #case 'xzspwc'
  and cqzt in ('01', '02')
  -- #break
  -- #case 'xtpzjds'
  and d.jlbh is not null
  -- #break
  -- #case 'xtpzajs'
  and r.ryzt = '02'
  -- #break
  -- #case 'xtdbwhz'
  and not EXISTS(select 1 from g2bajd.ZFBA_ZFGX_XTXX t2 where t2.rodm = 'CO100-R04' and t2.rwid = t.rwid)
-- #break
-- #end
group by t.rwmc, t.ajbh, t.ajywh, t.fqdwdm, t.fqdwmc, t.xtdwmc, t.lrsj
order by t.lrsj;

/*[sshjtj-zcxtdetailry]*/
/*#发起提捕协同流程人员数据#*/
select r.ryxm,
       r.zjhm,
       r.jdyymc,
       r.jdyydm,
       t.ajbh,
       t.ajywh,
       t.lrsj
from g2bajd.ZFBA_ZFGX_XTXX t
         left join g2bajd.ZFBA_ZFGX_DBPZXX d on t.rwid = d.sjrwid
         left join g2bajd.ZFBA_ZFGX_DBPZXX_XYRXX r on d.jlbh = r.dbjlbh
         left join zhag.zhag_dicvalue zd on zd.zdlb = '050901' and zd.dm = r.JDYYDM
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where t.jlbz = '1'
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  and rodm = 'CO100-R01'
  and tqpzdbsbh is not null
-- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and gx.dm like :dwdm || '%'
-- #switch :lx
  -- #case 'xtpzjdrs'
  and d.jlbh is not null
  -- #break
  -- #case 'xtpzrs'
  and r.ryzt = '02'
  -- #break
  -- #case 'xtjdbb'
  and r.ryzt = '03'
  and zd.ywfl = 'xjsf002'
  -- #break
  -- #case 'xtxdbb'
  and r.ryzt = '03'
  and zd.ywfl = 'xjsf001'
  -- #break
  -- #case 'xtcybb'
  and r.ryzt = '03'
  and zd.ywfl = 'xjsf003'
  -- #break
  -- #case 'xtsf'
  and exists(select 1
             from g2bajd.zfba_xs_sftzsxx sf
                      join g2bajd.zfba_gt_wffzxyr xyr on sf.fzxyrbh = xyr.wffzxyrbh
             where t.ajywh = sf.ajywh
               and xyr.zjhm = r.zjhm)
-- #break
-- #end
group by r.ryxm, r.zjhm, r.jdyymc, r.jdyydm, t.ajbh, t.ajywh, t.lrsj
order by t.lrsj;

/*[sshjtj-yssc]*/
/*#移送审查起诉节点数据统计#*/
with a as (select gx.sjjgdm,
                  gx.sjjgmc,
                  t.lrdwdm                                                       as dwdm,
                  nvl(jg.jc, jg.mc)                                              as lrdwmc,
                  gx.px,
                  count(distinct t.rwid)                                            zs,
                  count(case when cqzt = '00' then t.rwid end)                   as spz,
                  count(case when cqzt in ('01', '02') then t.rwid end)          as spwc,
                  count(distinct (case when h.jlbh is not null then t.ajbh end)) as tzajs,
                  count(distinct (case when h.jlbh is not null then t.ajbh end)) as tzcs,
                  count(distinct (case when r.ryzt = '05' then t.ajbh end))      as qss,
                  count(distinct (case when r.ryzt = '12' then r.zjhm end))      as pjs
           from g2bajd.ZFBA_ZFGX_XTXX t
                    left join g2bajd.ZFBA_ZFGX_DBPZXX d on t.rwid = d.sjrwid
                    left join g2bajd.ZFBA_ZFGX_DBPZXX_XYRXX r on d.jlbh = r.dbjlbh
                    left join g2bajd.ZFBA_ZFGX_DBBCCL C on c.SJRWID = t.RWID
                    left JOIN g2bajd.ZFBA_ZFGX_HKXX h on h.JLBH = C.YWBH and h.jlbz = '1' and h.YWLB = 'CO120-R63'
                    left join zhag.zhag_dicvalue zd on zd.zdlb = '050901' and zd.dm = r.JDYYDM
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = t.lrdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where t.jlbz = '1'
             and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             and rodm = 'CO120-R01'
             and qssbh is not null
-- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
           group by gx.sjjgdm, gx.sjjgmc, gx.px, t.LRDWDM, nvl(jg.jc, jg.mc)),
     b as (select gx.sjjgdm,
                  gx.sjjgmc,
                  t.lrdwdm                                                    as dwdm,
                  nvl(jg.jc, jg.mc)                                           as lrdwmc,
                  gx.px,
                  count(1)                                                       zs,
                  count(case when cqzt in ('00', '03', '05') then t.cqbh end) as spz,
                  count(case when cqzt in ('01', '02') then t.cqbh end)       as spwc,
                  count(case when cqzt = '04' then t.cqbh end)                as spth
           from g2bajd.ZFBA_XS_QSYJSXX t
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = t.lrdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where t.jlbz = '1'
             and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
           group by gx.sjjgdm, gx.sjjgmc, gx.px, t.LRDWDM, nvl(jg.jc, jg.mc))
select *
from (select nvl(a.sjjgdm, b.sjjgdm) as sjjgdm,
             nvl(a.sjjgmc, b.sjjgmc) as sjjgmc,
             nvl(a.dwdm, b.dwdm)     as dwdm,
             nvl(a.lrdwmc, a.lrdwmc) as dwmc,
             nvl(a.px, b.px)         as px,
             nvl(b.zs, 0)            as cqzs,
             nvl(b.spz, 0)           as cqspz,
             nvl(b.spwc, 0)          as cqspwc,
             nvl(b.spth, 0)          as cqspth,
             nvl(a.zs, 0)            as xtzs,
             nvl(a.spz, 0)           as xtspz,
             nvl(a.spwc, 0)          as xtspwc,
             nvl(a.tzajs, 0)         as tzajs,
             nvl(a.tzcs, 0)          as tzcs,
             nvl(a.qss, 0)           as qss,
             nvl(a.pjs, 0)           as pjs
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
      union all
      select '9999',
             '总数',
             '5301',
             '',
             '9999',
             sum(b.zs),
             sum(b.spz),
             sum(b.spwc),
             sum(b.spth),
             sum(a.zs),
             sum(a.spz),
             sum(a.spwc),
             sum(a.tzajs),
             sum(a.tzcs),
             sum(a.qss),
             sum(a.pjs)
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
-- #if :dwjb = 40
      union all
      select nvl(a.sjjgdm, b.sjjgdm),
             nvl(a.sjjgmc, b.sjjgmc),
             substr(nvl(a.sjjgdm, b.sjjgdm), 1, 6) as dwdm,
             '合计',
             nvl(a.px, b.px),
             sum(b.zs),
             sum(b.spz),
             sum(b.spwc),
             sum(b.spth),
             sum(a.zs),
             sum(a.spz),
             sum(a.spwc),
             sum(a.tzajs),
             sum(a.tzcs),
             sum(a.qss),
             sum(a.pjs)
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
-- #if :sjjgdm != '9999'
        and nvl(a.sjjgdm, b.sjjgdm) = :sjjgdm
      -- #fi
      group by nvl(a.sjjgdm, b.sjjgdm), nvl(a.sjjgmc, b.sjjgmc), nvl(a.px, b.px)
-- #fi
     )
order by px, dwdm
;
/*[sshjtj-ysscdetail]*/
/*#呈请移送审查起诉报告书数据#*/
select t.*
from g2bajd.ZFBA_XS_QSYJSXX t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where t.jlbz = '1'
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  -- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
-- #switch :lx
  -- #case 'cqzs'
  and 1 = '1'
-- #break
  -- #case 'cqspz'
  and cqzt in ('00', '03', '05')
-- #break
  -- #case 'cqspwc'
  and cqzt in ('01', '02')
-- #break
  -- #case 'cqspth'
  and cqzt = '04'
-- #break
--#end
  and gx.dm like :dwdm || '%'
order by t.lrsj desc
;

/*[sshjtj-xtysscdetail]*/
/*#发起提捕协同流程次数#*/
select t.rwmc,
       t.ajbh,
       t.ajywh,
       t.fqdwdm,
       t.fqdwmc,
       t.xtdwmc,
       t.lrsj
from g2bajd.ZFBA_ZFGX_XTXX t
         left join g2bajd.ZFBA_ZFGX_DBPZXX d on t.rwid = d.sjrwid
         left join g2bajd.ZFBA_ZFGX_DBPZXX_XYRXX r on d.jlbh = r.dbjlbh
         left join g2bajd.ZFBA_ZFGX_DBBCCL C on c.SJRWID = t.RWID
         left JOIN g2bajd.ZFBA_ZFGX_HKXX h on h.JLBH = C.YWBH and h.jlbz = '1' and h.YWLB = 'CO120-R63'
         left join zhag.zhag_dicvalue zd on zd.zdlb = '050901' and zd.dm = r.JDYYDM
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where t.jlbz = '1'
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  and rodm = 'CO120-R01'
  and qssbh is not null
-- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  -- #switch :lx
  -- #case 'xtzs'
  and 1 = '1'
  -- #break
  -- #case 'xtspz'
  and cqzt = '00'
  -- #break
  -- #case 'xtspwc'
  and cqzt in ('01', '02')
  -- #break
  -- #case 'tzcs'
  and h.jlbh is not null
  -- #break
  -- #case 'qss'
  and r.ryzt = '05'
  -- #break
  -- #case 'pjs'
  and r.ryzt = '12'
-- #break
-- #end
  and gx.dm like :dwdm || '%'
group by t.rwmc, t.ajbh, t.ajywh, t.fqdwdm, t.fqdwmc, t.xtdwmc, t.lrsj
order by t.lrsj desc
;

/*[sshjtj-xtysscdetailaj]*/
/*#发起提捕协同流程案件数#*/
select t.ajbh,
       t.ajywh,
       aj.ajmc,
       aj.ajlbmc,
       aj.zbdwdm,
       aj.zbdwmc
from g2bajd.ZFBA_ZFGX_XTXX t
         join g2bajd.ZFBA_ZFGX_DBBCCL C on c.SJRWID = t.RWID
         JOIN g2bajd.ZFBA_ZFGX_HKXX h on h.JLBH = C.YWBH and h.jlbz = '1' and h.YWLB = 'CO120-R63'
         left join g2bajd.zfba_gt_ajjbxx aj on aj.ajywh = t.ajywh
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.lrdwdm
where t.jlbz = '1'
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  and rodm = 'CO120-R01'
  and qssbh is not null
-- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and gx.dm like :dwdm || '%'
order by t.lrsj desc
;

/*[sjtj-xyrtjxx]*/
/*#犯罪嫌疑人年龄维度分析统计#*/
with a as (select gx.sjjgdm,
                  gx.sjjgmc,
                  r.lrdwdm                                                   as dwdm,
                  nvl(jg.jc, jg.mc)                                          as dwmc,
                  gx.px,
                  nullif(count(r.wffzxyrbh), 0)                              as zs,
                  count(case when nl < 14 then r.wffzxyrbh end)              as wm14,
                  count(case when nl >= 14 and nl < 16 then r.wffzxyrbh end) as wm16,
                  count(case when nl >= 16 and nl < 18 then r.wffzxyrbh end) as wm18,
                  count(case when nl < 18 then r.wffzxyrbh end)              as wcn,
                  count(case when nl >= 18 and nl < 60 then r.wffzxyrbh end) as xy60,
                  count(case when nl > 60 then r.wffzxyrbh end)              as dy60,
                  count(case when nl >= 18 then r.wffzxyrbh end)             as cn,
                  count(case when xb = '1' then r.wffzxyrbh end)             as man,
                  count(case when xb = '2' then r.wffzxyrbh end)             as wowen
           from g2bajd.zfba_gt_wffzxyr r
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = r.lrdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where r.jlbz = '1'
             and r.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
           group by gx.sjjgdm, gx.sjjgmc, gx.px, r.LRDWDM, nvl(jg.jc, jg.mc)),
     b as (select gx.sjjgdm,
                  gx.sjjgmc,
                  r.lrdwdm                                                  as dwdm,
                  nvl(jg.jc, jg.mc)                                         as lrdwmc,
                  gx.px,
                  nullif(count(r.wffzxyrbh), 0)                             as zs,
                  nullif(count(case when nl < 18 then r.wffzxyrbh end), 0)  as wcn,
                  nullif(count(case when nl >= 18 then r.wffzxyrbh end), 0) as cn
           from g2bajd.zfba_gt_wffzxyr r
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = r.lrdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where r.jlbz = '1'
             and r.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') - 30 and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss') - 30
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
           group by gx.sjjgdm, gx.sjjgmc, gx.px, r.LRDWDM, nvl(jg.jc, jg.mc)),
     c as (select gx.sjjgdm,
                  gx.sjjgmc,
                  r.lrdwdm                                                  as dwdm,
                  nvl(jg.jc, jg.mc)                                         as lrdwmc,
                  gx.px,
                  nullif(count(r.wffzxyrbh), 0)                             as zs,
                  nullif(count(case when nl < 18 then r.wffzxyrbh end), 0)  as wcn,
                  nullif(count(case when nl >= 18 then r.wffzxyrbh end), 0) as cn
           from g2bajd.zfba_gt_wffzxyr r
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = r.lrdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where r.jlbz = '1'
             and r.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') - 365 and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss') - 365
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
           group by gx.sjjgdm, gx.sjjgmc, gx.px, r.LRDWDM, nvl(jg.jc, jg.mc))
select *
from (select a.sjjgdm,
             a.sjjgmc,
             a.dwdm,
             a.dwmc,
             a.px,
             a.zs,
             round((a.zs - c.zs) * 100 / c.zs, 2)    zstb,
             round((a.zs - b.zs) * 100 / b.zs, 2)    zshb,
             a.wm14,
             round(a.wm14 * 100 / a.zs, 2)  as       wm14zb,
             a.wm16,
             round(a.wm16 * 100 / a.zs, 2)  as       wm16zb,
             a.wm18,
             round(a.wm18 * 100 / a.zs, 2)  as       wm18zb,
             a.wcn,
             round(a.wcn * 100 / a.zs, 2)   as       wcnzb,
             round((a.wcn - c.wcn) * 100 / c.wcn, 2) wcntb,
             round((a.wcn - b.wcn) * 100 / b.wcn, 2) wcnhb,
             a.xy60,
             round(a.xy60 * 100 / a.zs, 2)  as       xy60zb,
             a.dy60,
             round(a.dy60 * 100 / a.zs, 2)  as       dy60zb,
             a.cn,
             round(a.cn * 100 / a.zs, 2)    as       cnzb,
             round((a.cn - c.cn) * 100 / c.cn, 2)    cntb,
             round((a.cn - b.cn) * 100 / b.cn, 2)    cnhb,
             a.man,
             round(a.man * 100 / a.zs, 2)   as       manzb,
             a.wowen,
             round(a.wowen * 100 / a.zs, 2) as       womenzb
      from a
               left join b on a.sjjgdm = b.sjjgdm and a.dwdm = b.dwdm
               left join c on a.sjjgdm = c.sjjgdm and a.dwdm = c.dwdm
      union all
      select '9999',
             '总数',
             '5301',
             '',
             '9999',
             sum(a.zs),
             round((sum(a.zs) - sum(c.zs)) * 100 / sum(c.zs), 2)    zstb,
             round((sum(a.zs) - sum(b.zs)) * 100 / sum(b.zs), 2)    zshb,
             sum(a.wm14),
             round(sum(a.wm14) * 100 / sum(a.zs), 2)  as            wm14zb,
             sum(a.wm16),
             round(sum(a.wm16) * 100 / sum(a.zs), 2)  as            wm16zb,
             sum(a.wm18),
             round(sum(a.wm18) * 100 / sum(a.zs), 2)  as            wm18zb,
             sum(a.wcn),
             round(sum(a.wcn) * 100 / sum(a.zs), 2)   as            wcnzb,
             round((sum(a.wcn) - sum(c.wcn)) * 100 / sum(c.wcn), 2) wcntb,
             round((sum(a.wcn) - sum(b.wcn)) * 100 / sum(b.wcn), 2) wcnhb,
             sum(a.xy60),
             round(sum(a.xy60) * 100 / sum(a.zs), 2)  as            xy60zb,
             sum(a.dy60),
             round(sum(a.dy60) * 100 / sum(a.zs), 2)  as            dy60zb,
             sum(a.cn),
             round(sum(a.cn) * 100 / sum(a.zs), 2)    as            cnzb,
             round((sum(a.cn) - sum(c.cn)) * 100 / sum(c.cn), 2)    cntb,
             round((sum(a.cn) - sum(b.cn)) * 100 / sum(b.cn), 2)    cnhb,
             sum(a.man),
             round(sum(a.man) * 100 / sum(a.zs), 2)   as            manzb,
             sum(a.wowen),
             round(sum(a.wowen) * 100 / sum(a.zs), 2) as            womenzb
      from a
               left join b on a.sjjgdm = b.sjjgdm and a.dwdm = b.dwdm
               left join c on a.sjjgdm = c.sjjgdm and a.dwdm = c.dwdm
-- #if :dwjb = 40
      union all
      select a.sjjgdm,
             a.sjjgmc,
             substr(a.sjjgdm, 1, 6)                   as            dwdm,
             '合计',
             a.px,
             sum(a.zs),
             round((sum(a.zs) - sum(c.zs)) * 100 / sum(c.zs), 2)    zstb,
             round((sum(a.zs) - sum(b.zs)) * 100 / sum(b.zs), 2)    zshb,
             sum(a.wm14),
             round(sum(a.wm14) * 100 / sum(a.zs), 2)  as            wm14zb,
             sum(a.wm16),
             round(sum(a.wm16) * 100 / sum(a.zs), 2)  as            wm16zb,
             sum(a.wm18),
             round(sum(a.wm18) * 100 / sum(a.zs), 2)  as            wm18zb,
             sum(a.wcn),
             round(sum(a.wcn) * 100 / sum(a.zs), 2)   as            wcnzb,
             round((sum(a.wcn) - sum(c.wcn)) * 100 / sum(c.wcn), 2) wcntb,
             round((sum(a.wcn) - sum(b.wcn)) * 100 / sum(b.wcn), 2) wcnhb,
             sum(a.xy60),
             round(sum(a.xy60) * 100 / sum(a.zs), 2)  as            xy60zb,
             sum(a.dy60),
             round(sum(a.dy60) * 100 / sum(a.zs), 2)  as            dy60zb,
             sum(a.cn),
             round(sum(a.cn) * 100 / sum(a.zs), 2)    as            cnzb,
             round((sum(a.cn) - sum(c.cn)) * 100 / sum(c.cn), 2)    cntb,
             round((sum(a.cn) - sum(b.cn)) * 100 / sum(b.cn), 2)    cnhb,
             sum(a.man),
             round(sum(a.man) * 100 / sum(a.zs), 2)   as            manzb,
             sum(a.wowen),
             round(sum(a.wowen) * 100 / sum(a.zs), 2) as            womenzb
      from a
               left join b on a.sjjgdm = b.sjjgdm and a.dwdm = b.dwdm
               left join c on a.sjjgdm = c.sjjgdm and a.dwdm = c.dwdm
      where 1 = 1
-- #if :dwjb != '40'
        and a.sjjgdm = :sjjgdm
-- #fi
      group by a.sjjgdm, a.sjjgmc, a.px
-- #fi
     )
order by px, dwdm
;

/*[sjtj-xyrtjdetail]*/
/*#犯罪嫌疑人年龄维度分析统计详情数据#*/
select *
from g2bajd.zfba_gt_wffzxyr r
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = r.lrdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where r.jlbz = '1'
  and r.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  -- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  -- #switch :lx
  -- #case zs
  and 1 = 1
  -- #break
  -- #case wm14
  and r.nl < 14
  -- #break
  -- #case wm16
  and r.nl < 16
  and r.nl >= 14
  -- #break
  -- #case wm18
  and r.nl < 18
  and r.nl >= 16
  -- #break
  -- #case wcn
  and r.nl < 18
  -- #break
  -- #case xy60
  and r.nl < 60
  and r.nl >= 18
  -- #break
  -- #case dy60
  and r.nl >= 60
  -- #break
  -- #case cn
  and r.nl > 18
  -- #break
  -- #case man
  and r.xb = '1'
  -- #break
  -- #case wowen
  and r.xb = '2'
  -- #break
  -- #end
  and gx.dm like :dwdm || '%'
order by r.lrsj desc
;

/*[sjtj-gfajtjxx]*/
/*#高发刑事案件分析统计#*/
select a.ajlb,
       a.ajlbmc,
       a.sl,
       a.rn,
       round((b.sl - a.sl) * 100 / b.sl, 2) hb,
       round((c.sl - a.sl) * 100 / c.sl, 2) tb
from (select ajlb, ajlbmc, count(1) sl, rank() over ( order by count(1) desc)  as rn
      from g2bajd.zfba_gt_ajjbxx t
               join (select *
                     from zhag_sys_jggx
                     where sjjgjz = '38'
                       and sjjgjb <> '40'
                       and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                    on gx.dm = t.zbdwdm
      where jlbz = '1'
        and t.ajlx <> '11'
        and ajlb is not null
        and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
        and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
      group by ajlb, ajlbmc) a
         left join (select ajlb, ajlbmc, count(1) sl
                    from g2bajd.zfba_gt_ajjbxx t
                             join (select *
                                   from zhag_sys_jggx
                                   where sjjgjz = '38'
                                     and sjjgjb <> '40'
                                     and sjjgdm not in
                                         ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                                  on gx.dm = t.zbdwdm
                    where jlbz = '1'
                      and t.ajlx <> '11'
                      and ajlb is not null
                      and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
                      and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') - 30 and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss') - 30
                    group by ajlb, ajlbmc) b on a.ajlb = b.ajlb and a.ajlbmc = b.ajlbmc
         left join (select ajlb, ajlbmc, count(1) sl
                    from g2bajd.zfba_gt_ajjbxx t
                             join (select *
                                   from zhag_sys_jggx
                                   where sjjgjz = '38'
                                     and sjjgjb <> '40'
                                     and sjjgdm not in
                                         ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                                  on gx.dm = t.zbdwdm
                    where jlbz = '1'
                      and t.ajlx <> '11'
                      and ajlb is not null
                      and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
                      and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') - 365 and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss') - 365
                    group by ajlb, ajlbmc) c on a.ajlb = c.ajlb and a.ajlbmc = c.ajlbmc
where rn <= 10
order by rn;

/*[sjtj-gfajtjdetail]*/
/*#高发刑事案件分析统计详情数据#*/
select *
from g2bajd.zfba_gt_ajjbxx t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.zbdwdm
where jlbz = '1'
  and t.ajlx <> '11'
  and ajlb is not null
  and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
  and ajlb = :ajlb
  and lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
order by lrsj desc;

/*[sjtj-ajzttjxx]*/
/*#刑事案件办理状态统计#*/
with a as (select gx.sjjgdm,
                  gx.sjjgmc,
                  t.zbdwdm                                       as dwdm,
                  nvl(jg.jc, jg.mc)                              as dwmc,
                  gx.px,
                  count(case when t.ajzt = '14' then t.ajbh end) as sa,
                  count(case when t.ajzt = '22' then t.ajbh end) as la,
                  count(case when t.ajzt = '32' then t.ajbh end) as byla,
                  count(case when t.ajzt = '52' then t.ajbh end) as pa,
                  count(case when t.ajzt = '64' then t.ajbh end) as ysscqs,
                  count(case when t.ajzt = '42' then t.ajbh end) as cxaj,
                  count(case when t.ajzt = '82' then t.ajbh end) as ysaj
           from g2bajd.zfba_gt_ajjbxx t
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = t.zbdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where t.jlbz = '1'
             and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             and t.ajlx <> '11'
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
           group by gx.sjjgdm, gx.sjjgmc, gx.px, t.zbdwdm, nvl(jg.jc, jg.mc))
select *
from (select *
      from a
      union all
      select '9999',
             '总数',
             '5301',
             '',
             '9999',
             sum(sa),
             sum(la),
             sum(byla),
             sum(pa),
             sum(ysscqs),
             sum(cxaj),
             sum(ysaj)
      from a
-- #if :dwjb = 40
      union all
      select a.sjjgdm,
             a.sjjgmc,
             substr(a.sjjgdm, 1, 6) as dwdm,
             '合计',
             a.px,
             sum(sa),
             sum(la),
             sum(byla),
             sum(pa),
             sum(ysscqs),
             sum(cxaj),
             sum(ysaj)
      from a
      where 1 = 1
-- #if :sjjgdm != '9999'
        and a.sjjgdm = :sjjgdm
-- #fi
      group by a.sjjgdm, a.sjjgmc, a.px
         -- #fi
     )
order by px, dwdm
;

/*[sjtj-ajzttjdetail]*/
/*#刑事案件办理状态统计详情数据#*/
select *
from g2bajd.zfba_gt_ajjbxx t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.zbdwdm
where t.jlbz = '1'
  and t.ajlx <> '11'
  and ajlb is not null
  and gx.dm like :dwdm || '%'
  and ajzt = :ajzt
  -- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
order by t.lrsj desc;

/*[zdgx-tj]*/
/*#指定管辖案件情况统计#*/
with a as (select gx.sjjgdm,
                  gx.sjjgmc,
                  t.zbdwdm                                                       as dwdm,
                  nvl(jg.jc, jg.mc)                                              as dwmc,
                  gx.px,
                  count(t.ajbh)                                                  as zs,
                  count(case when t.zdlx = 'qxzdgx' then t.ajbh end)             as zszd,
                  count(case when t.zdlx in ('qxkzs', 'zszdgx') then t.ajbh end) as stzd
           from g2bajd.ZFBA_ZDGX_AJJBXX t
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = t.zbdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where t.jlbz = '1'
             and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             and t.ajlx <> '11'
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
           group by gx.sjjgdm, gx.sjjgmc, gx.px, t.zbdwdm, nvl(jg.jc, jg.mc))
select *
from (select *
      from a
      union all
      select '9999',
             '总数',
             '5301',
             '',
             '9999',
             sum(zs),
             sum(zszd),
             sum(stzd)
      from a
      union all
      select a.sjjgdm,
             a.sjjgmc,
             substr(a.sjjgdm, 1, 6) as dwdm,
             '合计',
             a.px,
             sum(zs),
             sum(zszd),
             sum(stzd)
      from a
      where 1 = 1
-- #if :sjjgdm != '9999'
        and a.sjjgdm = :sjjgdm
-- #fi
      group by a.sjjgdm, a.sjjgmc, a.px)
order by px, dwdm
;
/*[zdgx-detail]*/
/*#指定管辖案件情况统计详情数据#*/
select *
from g2bajd.ZFBA_ZDGX_AJJBXX t
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = t.zbdwdm
where t.jlbz = '1'
  and t.ajlx <> '11'
  and ajlb is not null
  and gx.dm like :dwdm || '%'
  -- #switch :lx
  -- #case zszd
  and t.zdlx = 'qxzdgx'
  -- #break
  -- #case stzd
  and t.zdlx in ('qxkzs', 'zszdgx')
  -- #break
  -- #end
  -- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
order by t.lrsj desc;

/*[lacajd-tj]*/
/*#立案监督、撤案监督统计#*/
with a as (select gx.sjjgdm,
                  gx.sjjgmc,
                  d.xtdwdm                                      as dwdm,
                  nvl(jg.jc, jg.mc)                             as dwmc,
                  gx.px,
                  count(x.jlbh)                                 as zs,
                  count(case when d.JSQS = '1' then x.jlbh end) as qss,
                  count(case when d.BLQS = '1' then x.jlbh end) as bls
           from g2bajd.ZFBA_ZFGX_HKXX x
                    left join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = d.xtdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where x.ywlb = 'CO380-R01'
             and x.rksj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
             and x.jlbz = '1'
           group by gx.sjjgdm, gx.sjjgmc, d.xtdwdm, nvl(jg.jc, jg.mc), gx.px),
     b as (select gx.sjjgdm,
                  gx.sjjgmc,
                  d.xtdwdm                                      as dwdm,
                  nvl(jg.jc, jg.mc)                             as dwmc,
                  gx.px,
                  count(x.jlbh)                                 as zs,
                  count(case when d.JSQS = '1' then x.jlbh end) as qss,
                  count(case when d.BLQS = '1' then x.jlbh end) as bls
           from g2bajd.ZFBA_ZFGX_HKXX x
                    left join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = d.xtdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where x.ywlb = 'CO381-R01'
             and x.rksj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
             and x.jlbz = '1'
           group by gx.sjjgdm, gx.sjjgmc, d.xtdwdm, nvl(jg.jc, jg.mc), gx.px)
select *
from (select nvl(a.sjjgdm, b.sjjgdm) as sjjgdm,
             nvl(a.sjjgmc, b.sjjgmc) as sjjgmc,
             nvl(a.dwdm, b.dwdm)     as dwdm,
             nvl(a.dwmc, a.dwmc)     as dwmc,
             nvl(a.px, b.px)         as px,
             nvl(a.zs, 0)            as lazs,
             nvl(a.qss, 0)           as laqss,
             nvl(a.bls, 0)           as labls,
             nvl(b.zs, 0)            as cazs,
             nvl(b.qss, 0)           as caqss,
             nvl(b.bls, 0)           as cabls
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
      union all
      select '9999',
             '总数',
             '5301',
             '',
             '9999',
             sum(a.zs),
             sum(a.qss),
             sum(a.bls),
             sum(b.zs),
             sum(b.qss),
             sum(b.bls)
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
      -- #if :dwjb = 40
      union all
      select nvl(a.sjjgdm, b.sjjgdm),
             nvl(a.sjjgmc, b.sjjgmc),
             substr(nvl(a.sjjgdm, b.sjjgdm), 1, 6) as dwdm,
             '合计',
             nvl(a.px, b.px),
             sum(a.zs),
             sum(a.qss),
             sum(a.bls),
             sum(b.zs),
             sum(b.qss),
             sum(b.bls)
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
        -- #if :sjjgdm != '9999'
        and nvl(a.sjjgdm, b.sjjgdm) = :sjjgdm
      -- #fi
      group by nvl(a.sjjgdm, b.sjjgdm), nvl(a.sjjgmc, b.sjjgmc), nvl(a.px, b.px)
-- #fi
     )
order by px, dwdm;

/*[lacajd-detail]*/
/*#立案监督、撤案监督统计详情数据#*/
select d.*, x.cldx
from g2bajd.ZFBA_ZFGX_HKXX x
         left join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = d.xtdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where x.ywlb = :ywlb
  -- #switch :lx
  -- #case qss
  and JSQS = '1'
  -- #break
  -- #case bls
  and BLQS = '1'
  -- #break
  -- #end
  and x.rksj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  -- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
  and x.jlbz = '1'
order by x.rksj desc
;

/*[lfxj-tj]*/
/*#两法衔接案件数据统计#*/
with a as (select gx.sjjgdm,
                  gx.sjjgmc,
                  t.fqdwdm                                      as dwdm,
                  nvl(jg.jc, jg.mc)                             as dwmc,
                  gx.px,
                  count(t.jlbh)                                 as zs,
                  count(case when d.JSQS = '1' then t.jlbh end) as qss,
                  count(case when d.BLQS = '1' then t.jlbh end) as bls
           from g2bajd.zfba_zfgx_xtxx t
                    left join g2bajd.ZFBA_ZFGX_DBPZXX d on t.rwid = d.sjrwid
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = t.fqdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where t.rodm = 'COB00-R01'
             and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
             and t.jlbz = '1'
           group by gx.sjjgdm, gx.sjjgmc, t.fqdwdm, nvl(jg.jc, jg.mc), gx.px),
     b as (select gx.sjjgdm,
                  gx.sjjgmc,
                  d.xtdwdm                                      as dwdm,
                  nvl(jg.jc, jg.mc)                             as dwmc,
                  gx.px,
                  count(x.jlbh)                                 as zs,
                  count(case when d.JSQS = '1' then x.jlbh end) as qss,
                  count(case when d.BLQS = '1' then x.jlbh end) as bls
           from g2bajd.ZFBA_ZFGX_HKXX x
                    left join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
                    join (select *
                          from zhag_sys_jggx
                          where sjjgjz = '38'
                            and sjjgjb <> '40'
                            and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
                         on gx.dm = d.xtdwdm
                    left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
           where x.ywlb = 'COB01-R01'
             and x.rksj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
             -- #if :dwjb != 40
             and gx.SJJGDM = :sjjgdm
-- #fi
             and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
             and x.jlbz = '1'
           group by gx.sjjgdm, gx.sjjgmc, d.xtdwdm, nvl(jg.jc, jg.mc), gx.px)
select *
from (select nvl(a.sjjgdm, b.sjjgdm) as sjjgdm,
             nvl(a.sjjgmc, b.sjjgmc) as sjjgmc,
             nvl(a.dwdm, b.dwdm)     as dwdm,
             nvl(a.dwmc, a.dwmc)     as dwmc,
             nvl(a.px, b.px)         as px,
             nvl(a.zs, 0)            as yszs,
             nvl(a.qss, 0)           as ysqss,
             nvl(a.bls, 0)           as ysbls,
             nvl(b.zs, 0)            as jszs,
             nvl(b.qss, 0)           as jsqss,
             nvl(b.bls, 0)           as jsbls
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
      union all
      select '9999',
             '总数',
             '5301',
             '',
             '9999',
             sum(a.zs),
             sum(a.qss),
             sum(a.bls),
             sum(b.zs),
             sum(b.qss),
             sum(b.bls)
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
      -- #if :dwjb = 40
      union all
      select nvl(a.sjjgdm, b.sjjgdm),
             nvl(a.sjjgmc, b.sjjgmc),
             substr(nvl(a.sjjgdm, b.sjjgdm), 1, 6) as dwdm,
             '合计',
             nvl(a.px, b.px),
             sum(a.zs),
             sum(a.qss),
             sum(a.bls),
             sum(b.zs),
             sum(b.qss),
             sum(b.bls)
      from a,
           b
      where a.sjjgdm = b.sjjgdm
        and a.dwdm = b.dwdm
        -- #if :sjjgdm != '9999'
        and nvl(a.sjjgdm, b.sjjgdm) = :sjjgdm
      -- #fi
      group by nvl(a.sjjgdm, b.sjjgdm), nvl(a.sjjgmc, b.sjjgmc), nvl(a.px, b.px)
-- #fi
     )
order by px, dwdm;

/*[lfxjys-detail]*/
/*#两法衔接案件数据统计(移送)详情数据#*/
select t.*
from g2bajd.zfba_zfgx_xtxx t
         left join g2bajd.ZFBA_ZFGX_DBBCCL d on t.rwid = d.sjrwid
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = d.fqdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where t.rodm = 'COB00-R01'
  -- #switch :lx
  -- #case qss
  and JSQS = '1'
  -- #break
  -- #case bls
  and BLQS = '1'
  -- #break
  -- #end
  and t.lrsj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  -- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
  and t.jlbz = '1'
order by t.lrsj desc
;

/*[lfxjjs-detail]*/
/*#两法衔接案件数据统计(接收)详情数据#*/
select d.*, x.cldx
from g2bajd.ZFBA_ZFGX_HKXX x
         left join g2bajd.ZFBA_ZFGX_DBBCCL d on x.ywbh = d.jlbh
         join (select *
               from zhag_sys_jggx
               where sjjgjz = '38'
                 and sjjgjb <> '40'
                 and sjjgdm not in ('530100240000', '530100270000', '530100400000', '530100260000')) gx
              on gx.dm = d.xtdwdm
         left join g2bajd.sa_jgxx jg on jg.dm = gx.dm
where x.ywlb = 'COB01-R01'
  -- #switch :lx
  -- #case qss
  and JSQS = '1'
  -- #break
  -- #case bls
  and BLQS = '1'
  -- #break
  -- #end
  and x.rksj between to_date(:kssj, 'yyyy-mm-dd hh24:mi:ss') and to_date(:jssj, 'yyyy-mm-dd hh24:mi:ss')
  -- #if :dwjb != 40
  and gx.SJJGDM = :sjjgdm
-- #fi
  and gx.dm in (select dm from table(zhag.FUNC_ZHAG_GET_JGXX(:dwdm)))
  and x.jlbz = '1'
order by x.rksj desc
;