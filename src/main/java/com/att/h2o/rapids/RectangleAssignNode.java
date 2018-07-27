package com.att.h2o.rapids;

public class RectangleAssignNode extends CompositeNode {

    public RectangleAssignNode(RapidsExprNode destination, RapidsExprNode values, RapidsExprNode colsExpr, RapidsExprNode rowsExpr) {
        super(":=");
        this.add(destination);
        this.add(values);
        this.add(colsExpr);
        this.add(rowsExpr);
    }
}
