package com.github.chengyuxing.sql.transaction;

/**
 * Transaction definition.
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

    public static Definition defaultDefinition() {
        Definition definition = new Definition();
        definition.setReadOnly(false);
        definition.setName("Trans_Unnamed");
        definition.setLevel(Level.READ_COMMITTED);
        return definition;
    }
}
