package com.att.h2o.rapids;

public class AssignNode extends CompositeNode {

    public AssignNode(FrameIdNode newId, RapidsExprNode frame) {
        super("assign");
        this.add(newId);
        this.add(frame);
    }
}
