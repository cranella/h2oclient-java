package com.att.h2o.rapids;

import java.util.ArrayList;
import java.util.List;

public class CompositeNode extends RapidsExprNode {

    protected List<RapidsExprNode> children;

    public CompositeNode(String value) {
        super(value);
        this.children = new ArrayList<>();
    }

    @Override
    public void add(RapidsExprNode exprNode) {
        children.add(exprNode);
    }

    @Override
    public void remove(RapidsExprNode exprNode) {
        children.remove(exprNode);
    }

    @Override
    public String toString() {
        return this.traverse();
    }

    protected String traverse() {
        StringBuilder ast = new StringBuilder("(");
        ast.append(this.value);
        for(RapidsExprNode child : children) {
            ast.append(" ");
            ast.append(child.toString());
        }
        return ast.append(")").toString();
    }
}
