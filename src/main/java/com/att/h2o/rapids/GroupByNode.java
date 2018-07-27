package com.att.h2o.rapids;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GroupByNode extends CompositeNode {

    protected int[] gbColIndices;
    protected List<AggregationNode> aggregations;


    public GroupByNode(RapidsExprNode frame, int[] gbColIndices) {
        super("GB");
        super.add(frame);
        this.gbColIndices = gbColIndices;
        this.aggregations = new ArrayList<>();
    }

    public void add(AggregationNode aggNode) {
        aggregations.add(aggNode);
    }

    public void remove(AggregationNode aggNode) {
        aggregations.remove(aggNode);
    }

    @Override
    public String toString() {
        StringBuilder ast = new StringBuilder("(");
        ast.append(this.value);
        ast.append(" ");
        ast.append(this.children.get(0).toString());
        ast.append(" ");
        ast.append(Arrays.toString(gbColIndices));
        for(AggregationNode agg : aggregations) {
            ast.append(" ");
            ast.append(agg.toString());
        }
        return ast.append(")").toString();
    }

}
