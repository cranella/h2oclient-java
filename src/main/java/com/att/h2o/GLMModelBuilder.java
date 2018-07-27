package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.GLMModelV3;
import water.bindings.pojos.GLMParametersV3;
import water.bindings.pojos.GLMV3;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.proxies.retrofit.ModelBuilders;

public class GLMModelBuilder extends H2OModelBuilder<GLMV3, GLMParametersV3, GLMModelV3> {
    public GLMModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new GLMParametersV3(), GLMModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new GLMCalls();
    }

    public GLMModelBuilder(GLMParametersV3 parameters) {
        super(parameters, GLMModelV3.class);
        calls = new GLMCalls();
    }

    private class GLMCalls implements Calls<GLMV3, GLMParametersV3> {
        @Override
        public Call<GLMV3> train(GLMParametersV3 params) {
            return ModelBuilders.Helper.trainGlm(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, GLMParametersV3 params) {
            return GridSearch.Helper.trainGlm(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<GLMV3> validateParameters(GLMParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersGlm(modelBuildersService, params);
        }
    }

}
