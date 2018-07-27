package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.DRFModelV3;
import water.bindings.pojos.DRFParametersV3;
import water.bindings.pojos.DRFV3;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.proxies.retrofit.ModelBuilders;

public class DRFModelBuilder extends H2OModelBuilder<DRFV3, DRFParametersV3, DRFModelV3>{

    public DRFModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new DRFParametersV3(), DRFModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new DRFCalls();
    }

    public DRFModelBuilder(DRFParametersV3 params) {
        super(params, DRFModelV3.class);
        calls = new DRFCalls();
    }

    private class DRFCalls implements Calls<DRFV3, DRFParametersV3> {
        @Override
        public Call<DRFV3> train(DRFParametersV3 params) {
            return ModelBuilders.Helper.trainDrf(modelBuildersService,params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, DRFParametersV3 params) {
            return GridSearch.Helper.trainDrf(gridSearchService,hyperParams, searchCriteria, params);
        }

        @Override
        public Call<DRFV3> validateParameters(DRFParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersDrf(modelBuildersService,params);
        }
    }

}
