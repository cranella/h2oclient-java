package com.att.h2o.rapids;

public class IntNode extends RapidsExprNode {

    public IntNode(int value) {
        super(Integer.toString(value));
    }
}
