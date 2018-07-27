package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.GBMModelV3;
import water.bindings.pojos.GBMParametersV3;
import water.bindings.pojos.GBMV3;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.proxies.retrofit.ModelBuilders;

public class GBMModelBuilder extends H2OModelBuilder<GBMV3, GBMParametersV3, GBMModelV3> {


    public GBMModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new GBMParametersV3(), GBMModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new GBMCalls();
    }

    public GBMModelBuilder(GBMParametersV3 params) {
        super(params, GBMModelV3.class);
        calls = new GBMCalls();
    }

    private class GBMCalls implements Calls<GBMV3, GBMParametersV3> {
        @Override
        public Call<GBMV3> train(GBMParametersV3 params) {
            return ModelBuilders.Helper.trainGbm(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, GBMParametersV3 params) {
            return GridSearch.Helper.trainGbm(gridSearchService,hyperParams,searchCriteria,params);
        }

        @Override
        public Call<GBMV3> validateParameters(GBMParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersGbm(modelBuildersService,params);
        }
    }

}
