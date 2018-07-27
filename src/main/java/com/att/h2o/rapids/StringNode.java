package com.att.h2o.rapids;

public class StringNode extends RapidsExprNode {

    public StringNode(String value) {
        super(value);
    }

    @Override
    public String toString() {
        return String.format("'%s'",value);
    }
}
