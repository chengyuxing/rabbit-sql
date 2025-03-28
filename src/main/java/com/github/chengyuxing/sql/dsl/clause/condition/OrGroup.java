package com.github.chengyuxing.sql.dsl.clause.condition;

import java.util.List;

public class OrGroup implements Criteria {
    private final List<Criteria> group;

    public OrGroup(List<Criteria> group) {
        this.group = group;
    }

    public List<Criteria> getGroup() {
        return group;
    }
}
