package com.att.h2o.rapids;

public class DoubleNode extends RapidsExprNode {

    public DoubleNode(double value) {
        super(Double.toString(value));
    }
}
