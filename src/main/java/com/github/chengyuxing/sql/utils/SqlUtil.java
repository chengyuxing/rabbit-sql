package com.github.chengyuxing.sql.utils;

import com.github.chengyuxing.common.tuple.Pair;
import com.github.chengyuxing.common.types.DataRow;
import com.github.chengyuxing.sql.types.ParamMode;
import com.github.chengyuxing.sql.types.Param;
import com.github.chengyuxing.sql.types.ValueWrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqlUtil {
    /**
     * 特殊字符用来防止字段名重复的问题
     */
    public static final String SEP = "\u02de";
    /**
     * 匹配命名参数
     */
    public static final Pattern ARG_PATTERN = Pattern.compile("[^:'\"|]:(?<name>[\\w." + SEP + "]+)", Pattern.MULTILINE);
    /**
     * 匹配字符串单引号里面的内容
     */
    public static final Pattern CHILD_STR_PATTERN = Pattern.compile("'[^']*'", Pattern.MULTILINE);

    public static String generateInsert(String tableName, Map<String, Param> data) {
        String fields = String.join(",", data.keySet());
        String holders = data.keySet().stream().
                map(key -> ":" + key)
                .collect(Collectors.joining(","));
        return "insert into " + tableName + " (" + fields + ") values (" + holders + ")";
    }

    public static String generateInsert(String tableName, DataRow row) {
        return generateInsert(tableName, row.toMap(Param::IN));
    }

    public static String generateUpdate(String tableName, Map<String, Param> data) {
        String sets = data.keySet().stream()
                .filter(key -> !key.contains(SEP))
                .map(key -> key + " = :" + key)
                .collect(Collectors.joining(", "));
        return "update " + tableName + " set " + sets;
    }

    /**
     * 解析获取预编译的SQL和按顺序的参数占位符名字
     *
     * @param sql 带参数占位符的SQL
     * @return 预编译SQL和参数名的集合
     */
    public static Pair<String, List<String>> getPreparedSqlAndIndexedArgNames(final String sql) {
        // 首先处理一下sql，先暂时将sql中的单引号内子字符串去除
        String _sql = sql;
        List<String> strChildren = new ArrayList<>();
        Matcher m1 = CHILD_STR_PATTERN.matcher(_sql);
        // 检查是否有子字符串
        int x = 0;
        while (m1.find()) {
            String strChild = m1.group();
            // 为每个子字符串按顺序编号并放入缓存内
            // 此处替换为特殊的占位符防止和用户写的冲突
            _sql = _sql.replace(strChild, SEP + (x++) + SEP);
            strChildren.add(strChild);
        }
        // 开始安全的处理参数占位符
        Matcher matcher = ARG_PATTERN.matcher(_sql);
        List<String> names = new ArrayList<>();
        while (matcher.find()) {
            String name = matcher.group("name");
            names.add(name);
            // 只替换第一个的原因是为防止参数名的包含关系
            _sql = _sql.replaceFirst(":" + name, "?");
        }

        // 最后再将一开始移除的子字符串重新放回到sql中
        for (int i = 0; i < strChildren.size(); i++) {
            _sql = _sql.replace(SEP + i + SEP, strChildren.get(i));
        }

        return Pair.of(_sql, names);
    }

    /**
     * 解除字段值的包装，获取真实的字段值
     *
     * @param value 包装的字段值
     * @return 字段值
     */
    public static Object unWrapValue(Object value) {
        if (value instanceof ValueWrap) {
            return ((ValueWrap) value).getValue();
        }
        return value;
    }

    /**
     * 解析SQL字符串
     *
     * @param sourceSql 原始sql字符串
     * @param args      参数
     */
    public static String resolveSqlPart(final String sourceSql, Map<String, Param> args) {
        AtomicReference<String> sourceSqlRef = new AtomicReference<>(sourceSql);
        args.keySet().forEach(k -> {
            ParamMode pm = args.get(k).getParamMode();
            if (pm == ParamMode.TEMPLATE) {
                String v = args.get(k).getValue().toString() + " ";
                sourceSqlRef.set(sourceSqlRef.get().replace("${" + k + "}", v));
            } else if (pm == ParamMode.IN || pm == ParamMode.IN_OUT) {
                Object v = args.get(k).getValue();
                if (v instanceof ValueWrap) {
                    ValueWrap wrapV = (ValueWrap) v;
                    sourceSqlRef.set(sourceSqlRef.get().replace(":" + k, wrapV.getStart() + " :" + k + wrapV.getEnd()));
                }
            }
        });
        return sourceSqlRef.get();
    }
}
