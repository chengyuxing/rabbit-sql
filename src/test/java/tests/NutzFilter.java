package tests;

import org.nutz.boot.starter.WebFilterFace;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;

public class NutzFilter implements WebFilterFace {
    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getPathSpec() {
        return null;
    }

    @Override
    public EnumSet<DispatcherType> getDispatches() {
        return EnumSet.of(DispatcherType.REQUEST, DispatcherType.FORWARD);
    }

    @Override
    public Filter getFilter() {
        return null;
    }

    @Override
    public Map<String, String> getInitParameters() {
        return null;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
