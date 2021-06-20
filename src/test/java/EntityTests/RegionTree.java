package EntityTests;

import com.github.chengyuxing.common.MenuTree;

import java.util.Map;

public class RegionTree extends MenuTree.Tree {

    public RegionTree(Map<?, ?> node) {
        super(node);
    }

    @Override
    protected String idKey() {
        return "id";
    }

    @Override
    protected String pidKey() {
        return "pid";
    }
}
