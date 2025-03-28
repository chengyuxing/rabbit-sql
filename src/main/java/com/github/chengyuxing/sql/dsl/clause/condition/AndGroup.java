package com.github.chengyuxing.sql.dsl.clause.condition;

import java.util.List;

public class AndGroup implements Criteria {
    private final List<Criteria> group;

    public AndGroup(List<Criteria> group) {
        this.group = group;
    }

    public List<Criteria> getGroup() {
        return group;
    }
}
