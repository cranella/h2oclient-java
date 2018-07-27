package com.att.h2o.rapids;

import java.util.StringJoiner;

public class ArrayNode extends CompositeNode {

    public ArrayNode() {
        super("");
    }

    @Override
    public String toString() {
        StringJoiner items = new StringJoiner(", ");
        for (RapidsExprNode child : children) {
            items.add(child.toString());
        }
        return "["+items.toString()+"]";
    }
}
