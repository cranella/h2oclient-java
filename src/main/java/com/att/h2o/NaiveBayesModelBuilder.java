package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.pojos.NaiveBayesModelV3;
import water.bindings.pojos.NaiveBayesParametersV3;
import water.bindings.pojos.NaiveBayesV3;
import water.bindings.proxies.retrofit.ModelBuilders;

public class NaiveBayesModelBuilder extends H2OModelBuilder<NaiveBayesV3, NaiveBayesParametersV3, NaiveBayesModelV3> {

    public NaiveBayesModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new NaiveBayesParametersV3(), NaiveBayesModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new NaiveBayesCalls();
    }

    public NaiveBayesModelBuilder(NaiveBayesParametersV3 parameters) {
        super(parameters, NaiveBayesModelV3.class);
        calls = new NaiveBayesCalls();
    }

    private class NaiveBayesCalls implements Calls<NaiveBayesV3, NaiveBayesParametersV3> {
        @Override
        public Call<NaiveBayesV3> train(NaiveBayesParametersV3 params) {
            return ModelBuilders.Helper.trainNaivebayes(modelBuildersService,params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, NaiveBayesParametersV3 params) {
            return GridSearch.Helper.trainNaivebayes(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<NaiveBayesV3> validateParameters(NaiveBayesParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersNaivebayes(modelBuildersService, params);
        }
    }

}
