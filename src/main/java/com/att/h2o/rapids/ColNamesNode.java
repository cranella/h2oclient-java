package com.att.h2o.rapids;

public class ColNamesNode extends CompositeNode {

    protected int[] colIndices;
    protected String[] names;

    public ColNamesNode(RapidsExprNode frame, int[] colIndices, String[] names) {
        super("colnames=");
        this.add(frame);
        this.colIndices = colIndices;
        this.names = names;
    }

    @Override
    public String toString() {
        StringBuilder ast = new StringBuilder("(");
        ast.append(this.value);
        ast.append(" ");
        ast.append(this.children.get(0).toString());
        ast.append(" [");
        for(int i : colIndices) {
            ast.append(String.format("%d ", i));
        }
        ast.append("] [");
        for(String s : names) {
            ast.append(String.format("'%s' ",s));
        }
        ast.append("]");
        return ast.append(")").toString();
    }

}
