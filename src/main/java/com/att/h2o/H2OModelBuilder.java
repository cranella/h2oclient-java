package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import com.att.h2o.proxies.retrofit.Grids;
import com.google.gson.Gson;
import retrofit2.Call;
import water.bindings.pojos.*;
import water.bindings.proxies.retrofit.ModelBuilders;

import java.util.*;

import static com.att.h2o.Util.keyToString;

public class H2OModelBuilder<B extends ModelBuilderSchema<? super P>, P extends ModelParametersSchemaV3, T extends ModelSchemaBaseV3> {

    protected P params;
    protected ModelBuilders modelBuildersService;
    protected GridSearch gridSearchService;
    protected Calls<B,P> calls;
    protected JobV3 job;
    protected Class<T> modelClass;
    protected Grids gridRetrievalService;

    public H2OModelBuilder(P parameters, Class<T> modelClass) {
        this.params = parameters;
        this.modelClass = modelClass;
        this.modelBuildersService = H2OConnection.getInstance().getService(ModelBuilders.class);
        this.gridSearchService = H2OConnection.getInstance().getService(GridSearch.class);
        this.gridRetrievalService = H2OConnection.getInstance().getService(Grids.class);
    }

    public H2OModel<T> build() throws H2OException {
        ValidationResults validationResults = this.validateParams();
        if(validationResults.getErrorCount() > 0) {
            ValidationMessageV3[] validationMessages = new ValidationMessageV3[validationResults.getErrorMessages().size()];
            validationMessages = validationResults.getErrorMessages().toArray(validationMessages);
            throw new H2OModelBuilderException(validationMessages,validationResults.getErrorCount(),params);
        }
        JobV3 job = train();
        String jobId = Util.keyToString(job.key);
        JobV3 jobResult = H2OConnection.getInstance().waitForJobCompletion(jobId);
        if(jobResult.status.equals("FAILED")) {
            throw new H2OException("Training failed\n" + jobResult.exception);
        }
        return retrieveModel();
    }


    public JobV3 train() throws H2OException {
        Call<B> call = calls.train(params);
        B builder = H2OConnection.getInstance().executeWithRetries(call);
        job = builder.job;
        return job;
    }

    public H2OModel<T> retrieveModel() throws H2OException {
        if (job != null) {
            if (job.dest.type.equals("Key<Model>")) {
                String modelId = keyToString(this.job.dest);
                return H2OConnection.getInstance().getModel(modelId, modelClass);
            }
        }
        return null;
    }

    public JobV3 gridSearch(Map<String,Object> hyperParameters, Map<String, Object> searchCriteria) throws H2OException {
        if(searchCriteria == null) {
            searchCriteria = new HashMap<>();
            searchCriteria.put("strategy","Cartesian");
        }
        Call<GridSearchSchema> call = calls.gridSearch((new Gson()).toJson(hyperParameters), (new Gson()).toJson(searchCriteria), params);
        GridSearchSchema gridSearch = H2OConnection.getInstance().executeWithRetries(call);
        job = gridSearch.job;
        return job;
    }

    public GridSchemaV99 retrieveGrid(String sortBy, boolean decreasing) throws H2OException {
        if(job != null) {
            if (job.dest.type.equals("Key<Grid>")) {
                Call<GridSchemaV99> call = gridRetrievalService.fetch(Util.keyToString(job.dest), sortBy, decreasing, null);
                return H2OConnection.getInstance().executeWithRetries(call);
            }
        }
        return null;
    }

    public GridSchemaV99 retrieveGrid() throws H2OException {
        if(job != null) {
            if (job.dest.type.equals("Key<Grid>")) {
                Call<GridSchemaV99> call = gridRetrievalService.fetch(Util.keyToString(job.dest));
                return H2OConnection.getInstance().executeWithRetries(call);
            }
        }
        return null;
    }

    public ValidationResults validateParams() throws H2OException {
        Call<B> call = calls.validateParameters(params);
        B builder = H2OConnection.getInstance().executeWithRetries(call);
        return new ValidationResults(builder.messages,builder.errorCount);
    }

    public JobV3 poll() throws H2OException {
        if(job != null) {
            job = H2OConnection.getInstance().poll(Util.keyToString(job.key));
            return job;
        }
        return null;
    }

    public P getParams() {
        return params;
    }

    public void setParams(P params) {
        this.params = params;
    }

    public void setTrainingFrame(String trainingFrame) {
        params.trainingFrame = Util.stringToFrameKey(trainingFrame);
    }

    public void setValidationFrame(String validationFrame) {
        params.validationFrame = Util.stringToFrameKey(validationFrame);
    }

    public void setResponseColumn(String responseColumn) {
        ColSpecifierV3 col = new ColSpecifierV3();
        col.columnName = responseColumn;
        params.responseColumn = col;
    }

    @Override
    public String toString() {
        return params.toString();
    }

    public static class ValidationResults {
        private List<ValidationMessageV3> messages;
        private int errorCount;

        public ValidationResults(ValidationMessageV3[] messages, int errorCount) {
            this.messages = Arrays.asList(messages);
            this.errorCount = errorCount;
        }

        public List<ValidationMessageV3> getMessages() {
            return messages;
        }

        public int getErrorCount() {
            return errorCount;
        }

        public List<ValidationMessageV3> getErrorMessages() {
            List<ValidationMessageV3> errors = new ArrayList<>();
            for(ValidationMessageV3 m : messages) {
                if(m.messageType.equals("ERRR")) {
                    errors.add(m);
                }
            }
            return errors;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for(ValidationMessageV3 m : messages) {
                sb.append(m.toString());
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    protected interface Calls<B extends ModelBuilderSchema<? super P>, P extends ModelParametersSchemaV3> {
        Call<B> train(P params);
        Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, P params);
        Call<B> validateParameters(P params);
    }
}
