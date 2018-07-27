package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.DeepLearningModelV3;
import water.bindings.pojos.DeepLearningParametersV3;
import water.bindings.pojos.DeepLearningV3;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.proxies.retrofit.ModelBuilders;

public class DeepLearningModelBuilder extends H2OModelBuilder<DeepLearningV3, DeepLearningParametersV3, DeepLearningModelV3> {
    public DeepLearningModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new DeepLearningParametersV3(), DeepLearningModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new DeepLearningCalls();
    }

    public DeepLearningModelBuilder(DeepLearningParametersV3 parameters) {
        super(parameters, DeepLearningModelV3.class);
        calls = new DeepLearningCalls();
    }

    private class DeepLearningCalls implements Calls<DeepLearningV3, DeepLearningParametersV3> {
        @Override
        public Call<DeepLearningV3> train(DeepLearningParametersV3 params) {
            return ModelBuilders.Helper.trainDeeplearning(modelBuildersService,params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, DeepLearningParametersV3 params) {
            return GridSearch.Helper.trainDeeplearning(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<DeepLearningV3> validateParameters(DeepLearningParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersDeeplearning(modelBuildersService,params);
        }
    }

}
