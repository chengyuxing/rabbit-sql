package com.github.chengyuxing.sql.yaml;

import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

/**
 * Yaml hyphen properties tool.
 */
public class HyphenatedPropertyUtil extends PropertyUtils {
    @Override
    public Property getProperty(Class<?> type, String name) {
        setSkipMissingProperties(true);
        return super.getProperty(type, camelize(name));
    }

    public static String camelize(String name) {
        int idx = name.indexOf("-");
        if (idx == -1) return name;
        if (idx + 1 >= name.length()) return name;
        String p = name.substring(idx, idx + 2);
        name = name.replace(p, p.substring(1).toUpperCase());
        return camelize(name);
    }
}
