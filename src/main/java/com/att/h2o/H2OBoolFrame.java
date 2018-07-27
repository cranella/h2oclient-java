package com.att.h2o;

import com.att.h2o.rapids.RapidsBuilder;

public class H2OBoolFrame extends H2OFrame {
    protected H2OBoolFrame(String frameId) {
        super(frameId);
    }

    protected H2OBoolFrame(RapidsBuilder rapidsBuilder) {
        super(rapidsBuilder);
    }

    public H2OBoolFrame and(H2OBoolFrame frame) {
        return (H2OBoolFrame) binOp("&", frame);
    }
    public H2OBoolFrame or(H2OBoolFrame frame) {
        return (H2OBoolFrame) binOp("|", frame);
    }
}
