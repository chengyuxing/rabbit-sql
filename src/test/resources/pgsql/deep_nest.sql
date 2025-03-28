/*[region]*/
select *
from test.region t
            where t.enable = true
--#if :a <> blank
    and t.a = :a
    --#if :a1 <> blank
        and t.a1 = :a1
        and t.a1 = :a1
        and t.a1 = :a1
    --#fi
    --#if :a2 <> blank
        and t.a2 = :a2
      --#choose
          --#when :xx <> blank
          and t.xx = :xx
          --#break
          --#when :yy <> blank
          and t.yy = :yy
          --#break
          --#default
          and t.zz = :zz
          --#break
        --#end
    --#fi
--#fi

--#choose
   --#when :x <> blank
    and t.x = :x
   --#break
   --#when :y <> blank
    and t.y = :y
   --#break
--#end

--#switch :name
  --#case blank
  and t.name = 'blank'
  --#break
  --#case 'chengyuxing'
  and t.name = 'chengyuxing'
  --#break
  --#default
  and t.name = 'unset'
  --#break
--#end

--#if :b <> blank
    and t.b = :b
--#fi
--#if :c <> blank
    --#if :c1 <> blank
        and t.c1 = :c1
        --#if :cc1 <> blank
            and t.cc1 = :cc1
        --#fi
        --#if :cc2 <> blank
            and t.cc2 = :cc2
        --#fi
    --#fi
    --#if :c2 <> blank
        and t.c2 = :c2
    --#fi
    and cc = :cc
--#fi

--#choose
    --#when :e <> blank
       and t.e = :e
       and t.ee = :e
       and t.eee = :e
    --#break
    --#when :f <> blank
       and t.f = :f
       --#if :ff <> blank
            and t.ff = :ff
            and t.ff2 = :ff
       --#else
            and t.ff3 = :ff and id in
            -- #for item  of :list  delimiter ',' open '(' close ')'
                :item
            -- #done
       --#fi
    --#break
    --#when :g <> blank
       and t.g = :g
    --#break
--#end
and x = :x
;;;

/*[choose]*/
select * from test.user where
-- #choose
    -- #when :id <> blank
        id = :id
    -- #break
    -- #when :name <> blank
        and name = :name
    -- #break
-- #end
;