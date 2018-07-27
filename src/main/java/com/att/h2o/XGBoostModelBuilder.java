package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.pojos.XGBoostModelV3;
import water.bindings.pojos.XGBoostParametersV3;
import water.bindings.pojos.XGBoostV3;
import water.bindings.proxies.retrofit.ModelBuilders;

public class XGBoostModelBuilder extends H2OModelBuilder<XGBoostV3, XGBoostParametersV3, XGBoostModelV3> {

    public XGBoostModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new XGBoostParametersV3(), XGBoostModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new XGBoostCalls();
    }

    public XGBoostModelBuilder(XGBoostParametersV3 parameters) {
        super(parameters, XGBoostModelV3.class);
        calls = new XGBoostCalls();
    }

    private class XGBoostCalls implements Calls<XGBoostV3, XGBoostParametersV3> {
        @Override
        public Call<XGBoostV3> train(XGBoostParametersV3 params) {
            return ModelBuilders.Helper.trainXgboost(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, XGBoostParametersV3 params) {
            return GridSearch.Helper.trainXgboost(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<XGBoostV3> validateParameters(XGBoostParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersXgboost(modelBuildersService, params);
        }
    }

}
