package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.pojos.StackedEnsembleModelV99;
import water.bindings.pojos.StackedEnsembleParametersV99;
import water.bindings.pojos.StackedEnsembleV99;
import water.bindings.proxies.retrofit.ModelBuilders;

public class StackedEnsembleModelBuilder extends H2OModelBuilder<StackedEnsembleV99, StackedEnsembleParametersV99, StackedEnsembleModelV99> {

    public StackedEnsembleModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new StackedEnsembleParametersV99(), StackedEnsembleModelV99.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new StackedEnsembleCalls();
    }

    public StackedEnsembleModelBuilder(StackedEnsembleParametersV99 parameters) {
        super(parameters, StackedEnsembleModelV99.class);
        calls = new StackedEnsembleCalls();
    }

    private class StackedEnsembleCalls implements Calls<StackedEnsembleV99, StackedEnsembleParametersV99> {
        @Override
        public Call<StackedEnsembleV99> train(StackedEnsembleParametersV99 params) {
            return ModelBuilders.Helper.trainStackedensemble(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, StackedEnsembleParametersV99 params) {
            return GridSearch.Helper.trainStackedensemble(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<StackedEnsembleV99> validateParameters(StackedEnsembleParametersV99 params) {
            return ModelBuilders.Helper.validate_parametersStackedensemble(modelBuildersService, params);
        }
    }

}
