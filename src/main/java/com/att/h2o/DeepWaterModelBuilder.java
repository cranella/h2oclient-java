package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.DeepWaterModelV3;
import water.bindings.pojos.DeepWaterParametersV3;
import water.bindings.pojos.DeepWaterV3;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.proxies.retrofit.ModelBuilders;

public class DeepWaterModelBuilder extends H2OModelBuilder<DeepWaterV3, DeepWaterParametersV3, DeepWaterModelV3> {

    public DeepWaterModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new DeepWaterParametersV3(), DeepWaterModelV3.class);
        setTrainingFrame(trainingFrame);
        setValidationFrame(validationFrame);
        setResponseColumn(responseColumn);
        calls = new DeepWaterCalls();
    }

    public DeepWaterModelBuilder(DeepWaterParametersV3 parameters) {
        super(parameters, DeepWaterModelV3.class);
        calls = new DeepWaterCalls();
    }

    private class DeepWaterCalls implements Calls<DeepWaterV3, DeepWaterParametersV3> {
        @Override
        public Call<DeepWaterV3> train(DeepWaterParametersV3 params) {
            return ModelBuilders.Helper.trainDeepwater(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, DeepWaterParametersV3 params) {
            return GridSearch.Helper.trainDeepwater(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<DeepWaterV3> validateParameters(DeepWaterParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersDeepwater(modelBuildersService, params);
        }
    }
}
