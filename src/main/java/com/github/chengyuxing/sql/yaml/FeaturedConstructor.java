package com.github.chengyuxing.sql.yaml;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Yaml custom tag for string join constructor.
 */
public class FeaturedConstructor extends Constructor {
    private final static Pattern VAR_PATTERN = Pattern.compile("\\$\\{\\s*(\\w+)\\s*}");

    public FeaturedConstructor() {
        super(new LoaderOptions());
        this.yamlConstructors.put(new Tag("!join"), new ConstructJoin(""));
        this.yamlConstructors.put(new Tag("!path"), new ConstructJoin("/"));
        this.setPropertyUtils(new HyphenatedPropertyUtil());
    }

    @Override
    protected String constructScalar(ScalarNode node) {
        String value = super.constructScalar(node);
        return resolveHolders(value);
    }

    @Override
    protected List<?> constructSequence(SequenceNode node) {
        //noinspection unchecked
        List<Object> list = (List<Object>) super.constructSequence(node);
        processSequence(list);
        return list;
    }

    @Override
    protected Map<Object, Object> constructMapping(MappingNode node) {
        Map<Object, Object> mapping = super.constructMapping(node);
        processMapping(mapping);
        return mapping;
    }

    private void processMapping(Map<Object, Object> mapping) {
        for (Map.Entry<Object, Object> entry : mapping.entrySet()) {
            if (entry.getValue() instanceof String) {
                entry.setValue(resolveHolders((String) entry.getValue()));
            } else if (entry.getValue() instanceof List) {
                //noinspection unchecked
                processSequence((List<Object>) entry.getValue());
            } else if (entry.getValue() instanceof Map) {
                //noinspection unchecked
                processMapping((Map<Object, Object>) entry.getValue());
            }
        }
    }

    private void processSequence(List<Object> sequence) {
        for (int i = 0; i < sequence.size(); i++) {
            Object value = sequence.get(i);
            if (value instanceof String) {
                sequence.set(i, resolveHolders((String) value));
            } else if (value instanceof List) {
                //noinspection unchecked
                processSequence((List<Object>) value);
            } else if (value instanceof Map) {
                //noinspection unchecked
                processMapping((Map<Object, Object>) value);
            }
        }
    }

    private String resolveHolders(String value) {
        if (!value.contains("${")) {
            return value;
        }
        Matcher matcher = VAR_PATTERN.matcher(value);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String holder = matcher.group(1);
            String varType = resolveVarType(holder);
            String resolvedValue;
            //noinspection SwitchStatementWithTooFewBranches
            switch (varType) {
                case "env":
                    String envKey = holder.substring(4);
                    resolvedValue = System.getenv(envKey);
                    if (resolvedValue == null) {
                        resolvedValue = "";
                    }
                    break;
                default:
                    resolvedValue = matcher.group(0);
                    break;
            }
            String safeValue = Matcher.quoteReplacement(resolvedValue);
            matcher.appendReplacement(sb, safeValue);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String resolveVarType(String varKey) {
        int dotIndex = varKey.indexOf('.');
        if (dotIndex == -1) {
            return varKey;
        }
        return varKey.substring(0, dotIndex);
    }

    private class ConstructJoin extends AbstractConstruct {
        private final String separator;

        private ConstructJoin(String separator) {
            this.separator = separator;
        }

        @Override
        public Object construct(Node node) {
            return constructSequence((SequenceNode) node)
                    .stream()
                    .map(path -> Optional.ofNullable(path).map(Object::toString).orElse(""))
                    .collect(Collectors.joining(separator));
        }
    }
}
