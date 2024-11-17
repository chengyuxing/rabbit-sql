package com.github.chengyuxing.sql.dsl.clause.condition;

import java.util.List;

public record OrGroup(List<Criteria> group) implements Criteria {
}
