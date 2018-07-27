package com.att.h2o.rapids;

public class AggregationNode extends RapidsExprNode {

    protected int colIndex;
    protected String naHandling;

    public AggregationNode(String aggOperation, int colIndex, String naHandling) {
        super(aggOperation);
        this.colIndex = colIndex;
        this.naHandling = naHandling;
    }

    @Override
    public String toString() {
        return String.format("'%s' %d '%s'", value, colIndex, naHandling);
    }
}
