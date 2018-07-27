package com.att.h2o.rapids;

public class RapidsExprNode {

    protected String value;

    public RapidsExprNode(String value) {
        this.value = value;
    }

    public void add(RapidsExprNode exprNode) {

    }
    public void remove(RapidsExprNode exprNode) {

    }

    @Override
    public String toString() {
        return value;
    }
}
