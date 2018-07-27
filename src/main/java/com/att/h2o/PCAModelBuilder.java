package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.pojos.PCAModelV3;
import water.bindings.pojos.PCAParametersV3;
import water.bindings.pojos.PCAV3;
import water.bindings.proxies.retrofit.ModelBuilders;

public class PCAModelBuilder extends H2OModelBuilder<PCAV3, PCAParametersV3, PCAModelV3> {

    public PCAModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new PCAParametersV3(), PCAModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new PCACalls();
    }

    public PCAModelBuilder(PCAParametersV3 parameters) {
        super(parameters, PCAModelV3.class);
        calls = new PCACalls();
    }

    private class PCACalls implements Calls<PCAV3, PCAParametersV3> {
        @Override
        public Call<PCAV3> train(PCAParametersV3 params) {
            return ModelBuilders.Helper.trainPca(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, PCAParametersV3 params) {
            return GridSearch.Helper.trainPca(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<PCAV3> validateParameters(PCAParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersPca(modelBuildersService,params);
        }
    }

}
