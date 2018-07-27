package com.att.h2o.rapids;


public class BinaryOpNode extends CompositeNode {

    public BinaryOpNode(String operator, RapidsExprNode leftNode, RapidsExprNode rightNode) {
        super(operator);
        this.add(leftNode);
        this.add(rightNode);
    }
}
