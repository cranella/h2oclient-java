package com.att.h2o.rapids;

public class UnaryOpNode extends CompositeNode{

    public UnaryOpNode(String operator, RapidsExprNode frame) {
        super(operator);
        this.add(frame);
    }
}
