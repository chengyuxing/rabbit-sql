package com.github.chengyuxing.sql.yaml;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * yaml文件自定标签字符串合并构造器
 */
public class JoinConstructor extends Constructor {

    public JoinConstructor() {
        super(new LoaderOptions());
        this.yamlConstructors.put(new Tag("!join"), new ConstructJoin(""));
        this.yamlConstructors.put(new Tag("!path"), new ConstructJoin("/"));
        this.setPropertyUtils(new HyphenatedPropertyUtil());
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
