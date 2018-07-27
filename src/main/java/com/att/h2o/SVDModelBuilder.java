package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.pojos.SVDModelV99;
import water.bindings.pojos.SVDParametersV99;
import water.bindings.pojos.SVDV99;
import water.bindings.proxies.retrofit.ModelBuilders;

public class SVDModelBuilder extends H2OModelBuilder<SVDV99, SVDParametersV99, SVDModelV99> {

    public SVDModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new SVDParametersV99(), SVDModelV99.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new SVDCalls();
    }

    public SVDModelBuilder(SVDParametersV99 parameters) {
        super(parameters, SVDModelV99.class);
        calls = new SVDCalls();
    }

    private class SVDCalls implements Calls<SVDV99, SVDParametersV99> {
        @Override
        public Call<SVDV99> train(SVDParametersV99 params) {
            return ModelBuilders.Helper.trainSvd(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, SVDParametersV99 params) {
            return GridSearch.Helper.trainSvd(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<SVDV99> validateParameters(SVDParametersV99 params) {
            return ModelBuilders.Helper.validate_parametersSvd(modelBuildersService, params);
        }
    }
}
