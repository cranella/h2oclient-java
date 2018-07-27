package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.AggregatorModelV99;
import water.bindings.pojos.AggregatorParametersV99;
import water.bindings.pojos.AggregatorV99;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.proxies.retrofit.ModelBuilders;

public class AggregatorModelBuilder extends H2OModelBuilder<AggregatorV99, AggregatorParametersV99, AggregatorModelV99> {

    public AggregatorModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new AggregatorParametersV99(), AggregatorModelV99.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new AggregatorCalls();
    }

    public AggregatorModelBuilder(AggregatorParametersV99 parameters) {
        super(parameters, AggregatorModelV99.class);
        calls = new AggregatorCalls();
    }

    @Override
    public ValidationResults validateParams() throws H2OException {
        try {
            return super.validateParams();
        } catch (H2OModelBuilderException e) {
            return new ValidationResults(e.getValidationMessages(), e.getErrorCount());
        }
    }

    private class AggregatorCalls implements Calls<AggregatorV99, AggregatorParametersV99> {
        @Override
        public Call<AggregatorV99> train(AggregatorParametersV99 params) {
            return ModelBuilders.Helper.trainAggregator(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, AggregatorParametersV99 params) {
            return GridSearch.Helper.trainAggregator(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<AggregatorV99> validateParameters(AggregatorParametersV99 params) {
            return ModelBuilders.Helper.validate_parametersAggregator(modelBuildersService, params);
        }
    }
}
