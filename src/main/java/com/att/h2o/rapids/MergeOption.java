package com.att.h2o.rapids;

public enum MergeOption {

    LEFT(true,false),
    RIGHT(false,true),
    INNER(false,false);

    private final boolean allLeft;
    private final boolean allRight;

    MergeOption(boolean allLeft, boolean allRight) {
        this.allLeft = allLeft;
        this.allRight = allRight;
    }

    public boolean isAllLeft() {
        return allLeft;
    }

    public boolean isAllRight() {
        return allRight;
    }
}
