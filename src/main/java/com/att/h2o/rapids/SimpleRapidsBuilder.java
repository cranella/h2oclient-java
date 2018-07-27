package com.att.h2o.rapids;

import com.att.h2o.H2OFrame;
import com.att.h2o.H2OBoolFrame;

public class SimpleRapidsBuilder implements RapidsBuilder {

    private RapidsExprNode ast;

    public SimpleRapidsBuilder(RapidsExprNode ast) {
        this.ast = ast;
    }

    public SimpleRapidsBuilder(String frameId) {
        this.ast = new FrameIdNode(frameId);
    }

    @Override
    public RapidsBuilder update(Object value, ArrayIndices rows, ArrayIndices cols) {
        _update(value, new IndicesNode(cols), new IndicesNode(rows));
        return this;
    }

    @Override
    public RapidsBuilder update(Object value, String col) {
        _update(value, new StringNode(col), new ArrayNode());
        return this;
    }

    @Override
    public RapidsBuilder update(Object value, String[] cols) {
        ArrayNode colArray = new ArrayNode();
        for(String s : cols) {
            colArray.add(new StringNode(s));
        }
        _update(value, colArray, new ArrayNode());
        return this;
    }

    @Override
    public RapidsBuilder update(Object value, H2OBoolFrame rows, ArrayIndices cols) {
        _update(value, new IndicesNode(cols), rows.getRapidsBuilder().getAst());
        return this;
    }

    @Override
    public RapidsBuilder update(Object value, H2OBoolFrame rows, String[] cols) {
        ArrayNode colArray = new ArrayNode();
        for(String s : cols) {
            colArray.add(new StringNode(s));
        }
        _update(value, colArray, rows.getRapidsBuilder().getAst());
        return this;
    }

    private void _update(Object value, RapidsExprNode colExpr, RapidsExprNode rowExpr) {
        if (value instanceof H2OFrame) {
            this.ast = new RectangleAssignNode(ast,((H2OFrame) value).getRapidsBuilder().getAst(), colExpr, rowExpr);
        } else if (value instanceof Double) {
            this.ast = new RectangleAssignNode(ast, new DoubleNode((Double) value), colExpr, rowExpr);
        } else if (value instanceof Integer) {
            this.ast = new RectangleAssignNode(ast, new IntNode((Integer) value), colExpr, rowExpr);
        } else if (value instanceof String) {
            this.ast = new RectangleAssignNode(ast, new StringNode((String) value), colExpr, rowExpr);
        } else {
            System.err.println("Incompatible type " + value.getClass().getName() + " passed as update value");
        }
    }

    @Override
    public RapidsBuilder select(ArrayIndices rows, ArrayIndices cols) {
        if(rows.isAll()) {
            this.ast = new BinaryOpNode("cols", ast, new IndicesNode(cols));
            return this;
        } else if(cols.isAll()) {
            this.ast = new BinaryOpNode("rows", ast, new IndicesNode(rows));
            return this;
        } else {
            this.ast = new BinaryOpNode("rows", new BinaryOpNode("cols", ast, new IndicesNode(cols)), new IndicesNode(rows));
            return this;
        }
    }

    @Override
    public RapidsBuilder select(String col) {
        this.ast = new BinaryOpNode("cols", ast, new StringNode(col));
        return this;
    }

    @Override
    public RapidsBuilder select(String[] cols) {
        ArrayNode colArray = new ArrayNode();
        for(String s : cols) {
            colArray.add(new StringNode(s));
        }
        this.ast = new BinaryOpNode("cols", ast, colArray);
        return this;
    }

    @Override
    public RapidsBuilder select(H2OBoolFrame rows) {
        this.ast = new BinaryOpNode("rows", ast, rows.getRapidsBuilder().getAst());
        return this;
    }

    @Override
    public RapidsBuilder assign(String id) {
        this.ast = new AssignNode(new FrameIdNode(id), ast);
        return this;
    }

    @Override
    public RapidsBuilder temp(String id) {
        this.ast = new TempAssignNode(new FrameIdNode(id), ast);
        return this;
    }

    @Override
    public RapidsBuilder binaryOp(String op, H2OFrame frame) {
        this.ast = new BinaryOpNode(op, ast,  frame.getRapidsBuilder().getAst());
        return this;
    }

    @Override
    public RapidsBuilder binaryOp(String op, double value) {
        this.ast = new BinaryOpNode(op, ast,  new DoubleNode(value));
        return this;
    }

    @Override
    public RapidsBuilder unaryOp(String func) {
        this.ast = new UnaryOpNode(func, ast);
        return this;
    }

    @Override
    public RapidsBuilder merge(H2OFrame frame, MergeOption mergeOption, int[] byLeft, int[] byRight, String mergeMethod) {
        this.ast = new MergeNode(ast, frame.getRapidsBuilder().getAst(),
                mergeOption.isAllLeft(), mergeOption.isAllRight(),
                byLeft, byRight, mergeMethod);
        return this;
    }

    @Override
    public RapidsBuilder cbind(H2OFrame cols) {
        this.ast = new BinaryOpNode("cbind", ast, cols.getRapidsBuilder().getAst());
        return this;
    }

    @Override
    public RapidsBuilder rbind(H2OFrame rows) {
        this.ast = new BinaryOpNode("rbind", ast, rows.getRapidsBuilder().getAst());
        return this;
    }

    @Override
    public RapidsBuilder groupBy(int[] cols) {
        this.ast = new GroupByNode(ast, cols);
        return this;
    }

    @Override
    public RapidsBuilder addAgg(String agg, int col, String naHandling) {
        if (this.ast instanceof GroupByNode) {
            ((GroupByNode) ast).add(new AggregationNode(agg, col, naHandling));
        }
        return this;
    }

    @Override
    public RapidsBuilder setNames(int[] cols, String[] names) {
        this.ast = new ColNamesNode(ast, cols, names);
        return this;
    }

    @Override
    public RapidsExprNode getAst() {
        return ast;
    }
}
