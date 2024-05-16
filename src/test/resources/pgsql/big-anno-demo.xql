/*
 Created by IntelliJ IDEA.
 User: chengyuxing
 Date: 2024/5/16
 Time: 13:23
 Typing "xql" keyword to get suggestions,
 e.g: "xql:new" will be create a sql fragment.
 @@@
 abc
 @@@
*/

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

/*[sshjtj-sla2]*/
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