package com.github.chengyuxing.sql.transaction;

/**
 * 事务定义
 */
public final class Definition {
    private String name;
    private boolean readOnly;
    private Level level = Level.READ_COMMITTED;

    public void setName(String name) {
        this.name = name;
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void setLevel(Level level) {
        this.level = level;
    }

    public String getName() {
        return name;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public int getLevel() {
        return level.getValue();
    }

    /**
     * 默认的事务级别
     *
     * @return 事务级别
     */
    public static Definition defaultDefinition() {
        Definition definition = new Definition();
        definition.setReadOnly(false);
        definition.setName("Trans_Unset");
        definition.setLevel(Level.READ_COMMITTED);
        return definition;
    }
}
