package com.att.h2o.rapids;

import java.util.Arrays;

public class MergeNode extends CompositeNode{

    protected boolean allLeft;
    protected boolean allRight;
    protected int[] byLeft;
    protected int[] byRight;
    protected String mergeMethod;

    public MergeNode(RapidsExprNode left, RapidsExprNode right, boolean allLeft, boolean allRight, int[] byLeft, int[] byRight, String mergeMethod) {
        super("merge");
        this.add(left);
        this.add(right);
        this.allLeft = allLeft;
        this.allRight = allRight;
        this.byLeft = byLeft;
        this.byRight = byRight;
        this.mergeMethod = mergeMethod;
    }

    @Override
    public String toString() {
        return String.format("(%s %s %s %b %b %s %s '%s')",
                value,
                children.get(0).toString(), children.get(1).toString(),
                allLeft, allRight,
                Arrays.toString(byLeft), Arrays.toString(byRight),
                mergeMethod);
    }
}
