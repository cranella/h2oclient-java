package com.att.h2o;

import com.att.h2o.proxies.retrofit.GridSearch;
import retrofit2.Call;
import water.bindings.pojos.GridSearchSchema;
import water.bindings.pojos.Word2VecModelV3;
import water.bindings.pojos.Word2VecParametersV3;
import water.bindings.pojos.Word2VecV3;
import water.bindings.proxies.retrofit.ModelBuilders;

public class Word2VecModelBuilder extends H2OModelBuilder<Word2VecV3, Word2VecParametersV3, Word2VecModelV3> {

    public Word2VecModelBuilder(String trainingFrame, String validationFrame, String responseColumn) {
        super(new Word2VecParametersV3(), Word2VecModelV3.class);
        super.setTrainingFrame(trainingFrame);
        super.setValidationFrame(validationFrame);
        super.setResponseColumn(responseColumn);
        calls = new Word2VecCalls();
    }

    public Word2VecModelBuilder(Word2VecParametersV3 parameters) {
        super(parameters, Word2VecModelV3.class);
        calls = new Word2VecCalls();
    }

    private class Word2VecCalls implements Calls<Word2VecV3, Word2VecParametersV3> {
        @Override
        public Call<Word2VecV3> train(Word2VecParametersV3 params) {
            return ModelBuilders.Helper.trainWord2vec(modelBuildersService, params);
        }

        @Override
        public Call<GridSearchSchema> gridSearch(String hyperParams, String searchCriteria, Word2VecParametersV3 params) {
            return GridSearch.Helper.trainWord2vec(gridSearchService, hyperParams, searchCriteria, params);
        }

        @Override
        public Call<Word2VecV3> validateParameters(Word2VecParametersV3 params) {
            return ModelBuilders.Helper.validate_parametersWord2vec(modelBuildersService, params);
        }
    }

}
