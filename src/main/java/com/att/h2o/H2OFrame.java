package com.att.h2o;

import com.att.h2o.proxies.retrofit.DownloadDataset;
import com.att.h2o.proxies.retrofit.FrameSummary;
import com.att.h2o.rapids.ArrayIndices;
import com.att.h2o.rapids.MergeOption;
import com.att.h2o.rapids.RapidsBuilder;
import com.att.h2o.rapids.SimpleRapidsBuilder;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import water.bindings.pojos.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.att.h2o.Util.keyToString;

public class H2OFrame {

    protected String frameId;
    protected FrameV4 summary;
    protected RapidsBuilder rapidsBuilder;

    protected static final Set<String> RELATIONAL_OPERATORS = new HashSet<>(
            Arrays.asList(">","<",">=","<=","==","!="));
    protected static final Set<String> LOGICAL_OPERATORS = new HashSet<>(Arrays.asList("|","&"));

    protected H2OFrame(String frameId) {
        this.frameId = frameId;
        this.rapidsBuilder = new SimpleRapidsBuilder(this.frameId);
    }

    protected H2OFrame(RapidsBuilder rapidsBuilder) {
        this.rapidsBuilder = rapidsBuilder;
    }

    public FrameV4 getSummary() throws H2OException {
        if(summary == null) {
            if(frameId == null) {
                execute();
            }
            summary = H2OConnection.getInstance().executeWithRetries(
                    H2OConnection.getInstance().getService(FrameSummary.class).summary(frameId));
        }
        return summary;
    }

    public String getFrameId() {
        if(frameId == null) {
            try {
                this.execute();
            } catch (H2OException e) {
                System.err.println("Error assigning frame id:");
                e.printStackTrace();
            }
        }
        return frameId;
    }


    public RapidsBuilder getRapidsBuilder() {
        return rapidsBuilder;
    }

    public H2OFrame assign(String frameId) throws H2OException {
        rapidsBuilder.assign(frameId);
        RapidsFrameV3 result = (RapidsFrameV3) H2OConnection.getInstance().evaluateRapidsExpression(rapidsBuilder.getAst().toString());
        this.rapidsBuilder = new SimpleRapidsBuilder(keyToString(result.key));
        this.frameId = keyToString(result.key);
        this.summary = null;
        return this;
    }

    public H2OFrame execute() throws H2OException {
        String tmpId = H2OConnection.getInstance().newTmp();
        rapidsBuilder.temp(tmpId);
        RapidsFrameV3 result = (RapidsFrameV3) H2OConnection.getInstance().evaluateRapidsExpression(rapidsBuilder.getAst().toString());
        this.rapidsBuilder = new SimpleRapidsBuilder(keyToString(result.key));
        this.frameId = keyToString(result.key);
        this.summary = null;
        return this;
    }

    //Unclear how H2O handles deep copying but below code seems to create new frame untied to original frame id
    public H2OFrame createCopy(String frameId) throws H2OException {
        RapidsBuilder rapids = (new SimpleRapidsBuilder(rapidsBuilder.getAst())).assign(frameId);
        RapidsFrameV3 result = (RapidsFrameV3) H2OConnection.getInstance().evaluateRapidsExpression(rapids.getAst().toString());
        return new H2OFrame(keyToString(result.key));
    }

    public H2OFrame select(int row, int col) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).select(new ArrayIndices(row), new ArrayIndices(col)));
    }

    public H2OFrame select(ArrayIndices rows, ArrayIndices cols) {
        if(rows.isAll() && cols.isAll()) {
            throw new IllegalArgumentException("Selection must be a subset of the current rows and/or cols");
        }
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).select(rows, cols));
    }

    public H2OFrame select(String col) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).select(col));
    }

    public H2OFrame select(String[] cols) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).select(cols));
    }

    public H2OFrame select(H2OBoolFrame boolFrame) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).select(boolFrame));
    }

    public H2OFrame update(ArrayIndices rows, ArrayIndices cols, Object value) {
        if(rows.isAll() && cols.isAll()) {
            throw new IllegalArgumentException("Selection must be a subset of the current rows and/or cols");
        }
        else if(rows.isAll()) {
            try {
                rows = new ArrayIndices(0, this.getSummary().rows);
            } catch (H2OException e) {
                throw new IllegalStateException("Cannot get row count. Try specifying range explicitly instead of using ArrayIndices.ALL");
            }
        }
        else if(cols.isAll()) {
            try {
                cols = new ArrayIndices(0, this.getSummary().numColumns);
            } catch (H2OException e) {
                throw new IllegalStateException("Cannot get col count. Try specifying range explicitly instead of using ArrayIndices.ALL");
            }
        }
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).update(value,rows, cols));
    }

    public H2OFrame update(String col, Object value) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).update(value, col));
    }

    public H2OFrame update(String[] cols, Object value) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).update(value, cols));
    }

    public H2OFrame update(H2OBoolFrame boolFrame, ArrayIndices cols, Object value) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).update(value, boolFrame, cols));
    }

    public H2OFrame update(H2OBoolFrame boolFrame, String[] cols, Object value) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).update(value, boolFrame, cols));
    }

    public H2OBoolFrame compare(String operator, double value) {
        if(!RELATIONAL_OPERATORS.contains(operator)) {
            throw new IllegalArgumentException("Operator must be one of: " + RELATIONAL_OPERATORS.toString());
        }
        return (H2OBoolFrame) binOp(operator, value);
    }

    public H2OBoolFrame compare(String operator, H2OFrame frame) {
        if(!RELATIONAL_OPERATORS.contains(operator)) {
            throw new IllegalArgumentException("Operator must be one of: " + RELATIONAL_OPERATORS.toString());
        }
        return (H2OBoolFrame) binOp(operator, frame);
    }

    public H2OFrame add(double value) {
        return binOp("+", value);
    }

    public H2OFrame add(H2OFrame frame) {
        return binOp("+", frame);
    }

    public H2OFrame subtract(double value) {
        return binOp("-", value);
    }

    public H2OFrame subtract(H2OFrame frame) {
        return binOp("-", frame);
    }

    public H2OFrame multiplyBy(double value) {
        return binOp("*", value);
    }

    public H2OFrame multiplyBy(H2OFrame frame) {
        return binOp("*", frame);
    }

    public H2OFrame divideBy(double value) {
        return binOp("/", value);
    }

    public H2OFrame divideBy(H2OFrame frame) {
        return binOp("/", frame);
    }

    public H2OFrame floorDivide(double value) {
        return binOp("intDiv", value);
    }

    public H2OFrame floorDivide(H2OFrame frame) {
        return binOp("intDiv", frame);
    }

    public H2OFrame mod(double value) {
        return binOp("%", value);
    }

    public H2OFrame mod(H2OFrame frame) {
        return binOp("%", frame);
    }

    public H2OFrame pow(double value) {
        return binOp("^", value);
    }

    public H2OFrame pow(H2OFrame frame) {
        return binOp("^", frame);
    }

    public H2OFrame asFactor()  {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).unaryOp("as.factor"));
    }

    public H2OFrame asNumeric() {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).unaryOp("as.numeric"));
    }

    public H2OFrame[] split(float[] ratios, String[] destinationFrames, int seed) throws H2OException {
        if(ratios == null) {
            throw new NullPointerException();
        }
        int numSlices = ratios.length + 1;
        if(destinationFrames != null) {
            if(destinationFrames.length != numSlices) {
                throw new IllegalArgumentException("Number of destination frames must be one more than the number of provided ratios");
            }
        }
        float lastBoundary = 0;
        float[] boundaries = new float[ratios.length];
        for(int i=0; i<ratios.length; i++) {
            if(ratios[i] < 0) {
                throw new IllegalArgumentException("Ratio must be greater than 0: " + ratios[i]);
            }
            float boundary = lastBoundary + ratios[i];
            if(boundary >= 1) {
                throw new IllegalArgumentException("Ratios must add up to less than 1");
            }
            boundaries[i] = boundary;
            lastBoundary = boundary;
        }
        H2OFrame tmpSplitter = (new H2OFrame(new SimpleRapidsBuilder(this.getFrameId()).binaryOp("h2o.runif",seed))).execute();
        H2OFrame[] newFrames = new H2OFrame[numSlices];
        H2OFrame tmpSplit;
        for(int i=0; i<numSlices; i++) {
            if(i == 0) {
                //lower boundary is 0
                tmpSplit = this.select(tmpSplitter.compare("<=",boundaries[i]));
            } else if (i == numSlices-1) {
                //upper boundary is 1
                tmpSplit = this.select(tmpSplitter.compare(">",boundaries[i-1]));
            } else {
                tmpSplit = this.select(tmpSplitter.compare(">",boundaries[i-1]).and(tmpSplitter.compare("<=",boundaries[i])));
            }

            if(destinationFrames == null) {
                newFrames[i] = tmpSplit.execute();
            } else {
                newFrames[i] = tmpSplit.assign(destinationFrames[i]);
            }
        }
        return newFrames;
    }

    public Object flatten() throws H2OException {
        FrameV4 summary = getSummary();
        if(summary.numColumns != 1 || summary.rowCount != 1) {
            throw new IllegalStateException("Frame dimensions must be 1x1");
        }
        RapidsBuilder rapids = new SimpleRapidsBuilder(rapidsBuilder.getAst()).unaryOp("flatten");
        RapidsSchemaV3 result = H2OConnection.getInstance().evaluateRapidsExpression(rapids.getAst().toString());
        if(result instanceof RapidsNumberV3) {
            return ((RapidsNumberV3) result).scalar;
        } else if (result instanceof RapidsStringV3) {
            return ((RapidsStringV3) result).string;
        } else {
            throw new H2OException("Scalar type unsupported for Rapids schema " + result.getClass() );
        }
    }

    public H2OGroupBy groupBy(String[] columnNames) {
        return new H2OGroupBy(this, namesToColIndices(columnNames));
    }

    public H2OGroupBy groupBy(int[] columnIndices) {
        return new H2OGroupBy(this,columnIndices);
    }


    public H2OFrame rowbind(H2OFrame frame) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).rbind(frame));
    }

    public H2OFrame colbind(H2OFrame frame) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).cbind(frame));
    }

    public H2OFrame merge(H2OFrame frame, MergeOption mergeOption, int[] byLeft, int[] byRight) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).merge(frame,mergeOption,byLeft,byRight,"auto"));
    }

    public H2OFrame merge(H2OFrame frame, MergeOption mergeOption, String[] byLeft, String[] byRight) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst())
                .merge(frame,mergeOption,namesToColIndices(byLeft), frame.namesToColIndices(byRight),"auto"));
    }

    public FileDownloadListener download(File file) throws H2OException, IOException {
        if (frameId == null) {
            execute();
        }
        if(!file.createNewFile()) {
            if (!file.canWrite()) {
                throw new IllegalArgumentException("Cannot write to file " + file.getPath());
            }
        }
        DownloadDataset downloadService = H2OConnection.getInstance().getService(DownloadDataset.class);
        Call<ResponseBody> call = downloadService.fetch(frameId);
        FileDownloadListener listener = new FileDownloadListener();
        call.enqueue(new Callback<ResponseBody>() {
            @Override
            public void onResponse(Call<ResponseBody> call, Response<ResponseBody> response) {
                if(response.isSuccessful()) {
                    Util.writeFile(response.body(), file, listener);
                } else {
                    listener.setError("Server error: " + response.code() + " " + response.message());
                }
            }
            @Override
            public void onFailure(Call<ResponseBody> call, Throwable throwable) {
                listener.setError(throwable.toString());
            }
        });

        return listener;
    }

    public H2OFrame setColNames(int[] indices, String[] names) {
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).setNames(indices, names));
    }

    public H2OFrame setColNames(String[] oldNames, String[] newNames) {
        return  new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).setNames(namesToColIndices(oldNames), newNames));
    }


    protected H2OFrame binOp(String op, H2OFrame value) {
        if(RELATIONAL_OPERATORS.contains(op) || LOGICAL_OPERATORS.contains(op)) {
            return new H2OBoolFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).binaryOp(op, value));
        }
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).binaryOp(op, value));
    }

    protected H2OFrame binOp(String op, double value) {
        if(RELATIONAL_OPERATORS.contains(op) || LOGICAL_OPERATORS.contains(op)) {
            return new H2OBoolFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).binaryOp(op, value));
        }
        return new H2OFrame(new SimpleRapidsBuilder(rapidsBuilder.getAst()).binaryOp(op, value));
    }

    protected int[] namesToColIndices(String[] cols) {
        FrameV4 frameInfo;
        try {
            frameInfo = getSummary();
        } catch (H2OException e) {
            throw new IllegalStateException("Cannot get columns");
        }
        int[] colIndices = new int[cols.length];
        for(int i=0; i<cols.length; i++) {
            colIndices[i] = getColIndex(frameInfo.columns,cols[i]);
        }
        return colIndices;
    }

    public ColV3 getColSummary(String colName) {
        FrameV4 frameInfo;
        try {
            frameInfo = getSummary();
        } catch (H2OException e) {
            throw new IllegalStateException("Cannot get columns");
        }
        int index = getColIndex(frameInfo.columns, colName);
        return frameInfo.columns[index];
    }

    private int getColIndex(ColV3[] cols, String colName) {
        for(int i=0; i<cols.length; i++) {
            if(cols[i].label.equals(colName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("Invalid column name '%s'",colName));
    }

    @Override
    public String toString() {
        try {
            return getSummary().toString();
        } catch (H2OException e) {
            return e.toString();
        }
    }
}

