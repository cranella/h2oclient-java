package com.att.h2o;

import com.att.h2o.rapids.GroupByNode;
import com.att.h2o.rapids.RapidsBuilder;
import com.att.h2o.rapids.SimpleRapidsBuilder;
import com.att.h2o.rapids.AggregationNode;
import water.bindings.pojos.RapidsFrameV3;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class H2OGroupBy {

    protected H2OFrame frame;
    protected RapidsBuilder rapidsBuilder;
    protected static final Set<String> NA_HANDLING_OPTIONS = new HashSet<>(
            Arrays.asList("all","rm","ignore"));

    protected H2OGroupBy(H2OFrame frame, int[] cols) {
        this.frame = frame;
        this.rapidsBuilder = new SimpleRapidsBuilder(frame.getRapidsBuilder().getAst());
        this.rapidsBuilder.groupBy(cols);
    }


    public H2OGroupBy min(int[] cols, String naHandling) {
        return this.addAgg("min", cols, naHandling);
    }

    public H2OGroupBy min(String[] cols, String naHandling) {
        return this.addAgg("min", frame.namesToColIndices(cols), naHandling);
    }

    public H2OGroupBy max(int[] cols, String naHandling) {
        return this.addAgg("max", cols, naHandling);
    }

    public H2OGroupBy max(String[] cols, String naHandling) {
        return this.addAgg("max", frame.namesToColIndices(cols), naHandling);
    }

    public H2OGroupBy mean(int[] cols, String naHandling) {
        return this.addAgg("mean", cols, naHandling);
    }

    public H2OGroupBy mean(String[] cols, String naHandling) {
        return this.addAgg("mean", frame.namesToColIndices(cols), naHandling);
    }

    public H2OGroupBy count(String naHandling) {
        return this.addAgg("nrow",new int[] {0}, naHandling);
    }

    public H2OGroupBy sum(int[] cols, String naHandling) {
        return this.addAgg("sum", cols, naHandling);
    }

    public H2OGroupBy sum(String[] cols, String naHandling) {
        return this.addAgg("sum", frame.namesToColIndices(cols), naHandling);
    }

    public H2OGroupBy stdDev(int[] cols, String naHandling) {
        return this.addAgg("sdev",cols, naHandling);
    }

    public H2OGroupBy stdDev(String[] cols, String naHandling) {
        return this.addAgg("sdev", frame.namesToColIndices(cols), naHandling);
    }

    public H2OGroupBy var(int[] cols, String naHandling) {
        return this.addAgg("var", cols, naHandling);
    }

    public H2OGroupBy var(String[] cols, String naHandling) {
        return this.addAgg("var", frame.namesToColIndices(cols), naHandling);
    }

    public H2OGroupBy sumSquares(int[] cols, String naHandling) {
        return this.addAgg("sumSquares", cols, naHandling);
    }

    public H2OGroupBy sumSquares(String[] cols, String naHandling) {
        return this.addAgg("sumSquares", frame.namesToColIndices(cols), naHandling);
    }

    public H2OGroupBy mode(int[] cols, String naHandling) {
        return this.addAgg("mode", cols, naHandling);
    }

    public H2OGroupBy mode(String[] cols, String naHandling) {
        return this.addAgg("mode", frame.namesToColIndices(cols), naHandling);
    }

    public H2OFrame getFrame(String frameId) throws H2OException {
        rapidsBuilder.assign(frameId);
        RapidsFrameV3 result = (RapidsFrameV3) H2OConnection.getInstance().evaluateRapidsExpression(rapidsBuilder.getAst().toString());
        return new H2OFrame(Util.keyToString(result.key));
    }

    public H2OFrame getFrame() throws H2OException {
        String tmpId = H2OConnection.getInstance().newTmp();
        rapidsBuilder.temp(tmpId);
        RapidsFrameV3 result = (RapidsFrameV3) H2OConnection.getInstance().evaluateRapidsExpression(rapidsBuilder.getAst().toString());
        return new H2OFrame(Util.keyToString(result.key));
    }

    public RapidsBuilder getRapidsBuilder() {
        return rapidsBuilder;
    }

    protected H2OGroupBy addAgg(String agg, int[] cols, String naHandling) {
        if(!NA_HANDLING_OPTIONS.contains(naHandling)) {
            throw new IllegalArgumentException("NA handling selection must be one of: 'all' 'ignore' 'rm'");
        }
        for(int col : cols) {
            ((GroupByNode) rapidsBuilder.getAst()).add(new AggregationNode(agg, col, naHandling));
        }
        return this;
    }

    @Override
    public String toString() {
        return rapidsBuilder.getAst().toString();
    }
}
