package com.att.h2o.rapids;

public class TempAssignNode extends CompositeNode{

    public TempAssignNode(FrameIdNode newId, RapidsExprNode frame) {
        super("tmp=");
        this.add(newId);
        this.add(frame);
    }
}
