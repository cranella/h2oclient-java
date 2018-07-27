package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.*;
import water.bindings.proxies.retrofit.ModelBuilders;

public class GLRMModelBuilder extends H2OModelBuilder<GLRMV3, GLRMParametersV3, GLRMModelV3> {

    public GLRMModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new GLRMParametersV3(), GLRMModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new GLRMCalls();
    }

    public GLRMModelBuilder(GLRMParametersV3 parameters) {
        super(parameters, GLRMModelV3.class);
        calls = new GLRMCalls();
    }

    private class GLRMCalls implements Calls<GLRMV3, GLRMParametersV3> {
        @Override
        public Call<GLRMV3> train(GLRMParametersV3 params) {
            return ModelBuilders.Helper.trainGlrm(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, GLRMParametersV3 params) {
            return GridSearch.Helper.trainGlrm(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<GLRMV3> validateParameters(GLRMParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersGlrm(modelBuildersService, params);
        }
    }

}
