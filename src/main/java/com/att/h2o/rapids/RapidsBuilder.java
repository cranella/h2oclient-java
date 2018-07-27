package com.att.h2o.rapids;

import com.att.h2o.H2OBoolFrame;
import com.att.h2o.H2OFrame;

public interface RapidsBuilder {

    RapidsBuilder select(ArrayIndices rows, ArrayIndices cols);
    RapidsBuilder select(String col);
    RapidsBuilder select(String cols[]);
    RapidsBuilder select(H2OBoolFrame rows);

    RapidsBuilder update(Object value, ArrayIndices rows, ArrayIndices cols);
    RapidsBuilder update(Object value, String col);
    RapidsBuilder update(Object value, String[] cols);
    RapidsBuilder update(Object value, H2OBoolFrame rows, ArrayIndices cols);
    RapidsBuilder update(Object value, H2OBoolFrame rows, String[] cols);

    RapidsBuilder assign(String id);
    RapidsBuilder temp(String id);

    RapidsBuilder binaryOp(String op, H2OFrame frame);
    RapidsBuilder binaryOp(String op, double value);

    RapidsBuilder unaryOp(String func);

    RapidsBuilder merge(H2OFrame frame, MergeOption mergeOption, int[] byX, int[] byY, String mergeMethod);

    RapidsBuilder cbind(H2OFrame cols);
    RapidsBuilder rbind(H2OFrame rows);

    RapidsBuilder groupBy(int[] cols);
    RapidsBuilder addAgg(String agg, int col, String naHandling);

    RapidsBuilder setNames(int[] cols, String[] names);

    RapidsExprNode getAst();
}
