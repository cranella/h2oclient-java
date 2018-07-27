package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.pojos.KMeansModelV3;
import water.bindings.pojos.KMeansParametersV3;
import water.bindings.pojos.KMeansV3;
import water.bindings.proxies.retrofit.ModelBuilders;

public class KMeansModelBuilder extends H2OModelBuilder<KMeansV3, KMeansParametersV3, KMeansModelV3> {

    public KMeansModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new KMeansParametersV3(), KMeansModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new KMeansCalls();
    }

    public KMeansModelBuilder(KMeansParametersV3 parameters) {
        super(parameters, KMeansModelV3.class);
        calls = new KMeansCalls();
    }

    private class KMeansCalls implements Calls<KMeansV3, KMeansParametersV3> {
        @Override
        public Call<KMeansV3> train(KMeansParametersV3 params) {
            return ModelBuilders.Helper.trainKmeans(modelBuildersService,params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, KMeansParametersV3 params) {
            return GridSearch.Helper.trainKmeans(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<KMeansV3> validateParameters(KMeansParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersKmeans(modelBuildersService,params);
        }
    }

}
