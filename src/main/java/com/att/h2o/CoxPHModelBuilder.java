package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.CoxPHModelV3;
import water.bindings.pojos.CoxPHParametersV3;
import water.bindings.pojos.CoxPHV3;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.proxies.retrofit.ModelBuilders;

public class CoxPHModelBuilder extends H2OModelBuilder<CoxPHV3,CoxPHParametersV3,CoxPHModelV3> {

    public CoxPHModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new CoxPHParametersV3(), CoxPHModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new CoxPHCalls();
    }

    public CoxPHModelBuilder(CoxPHParametersV3 parameters) {
        super(parameters, CoxPHModelV3.class);
        calls = new CoxPHCalls();
    }

    private class CoxPHCalls implements Calls<CoxPHV3,CoxPHParametersV3> {
        @Override
        public Call<CoxPHV3> train(CoxPHParametersV3 params) {
            return ModelBuilders.Helper.trainCoxph(modelBuildersService,params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, CoxPHParametersV3 params) {
            return GridSearch.Helper.trainCoxph(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<CoxPHV3> validateParameters(CoxPHParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersCoxph(modelBuildersService,params);
        }
    }

}
