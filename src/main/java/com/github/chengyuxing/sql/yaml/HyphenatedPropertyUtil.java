package com.github.chengyuxing.sql.yaml;

import com.github.chengyuxing.common.utils.StringUtil;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

/**
 * Yaml hyphen properties tool.
 */
public class HyphenatedPropertyUtil extends PropertyUtils {
    @Override
    public Property getProperty(Class<?> type, String name) {
        setSkipMissingProperties(true);
        return super.getProperty(type, StringUtil.camelize(name));
    }
}
