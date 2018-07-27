package com.att.h2o;

import com.google.gson.GsonBuilder;
import com.att.h2o.proxies.retrofit.DownloadModels;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import water.bindings.pojos.JobV3;
import water.bindings.pojos.ModelMetricsListSchemaV3;
import water.bindings.pojos.ModelSchemaBaseV3;
import water.bindings.proxies.retrofit.Predictions;

import java.io.File;
import java.io.IOException;

import static com.att.h2o.Util.keyToString;
import static com.att.h2o.Util.stringToFrameKey;
import static com.att.h2o.Util.writeFile;

public class H2OModel<T extends ModelSchemaBaseV3> {

    protected T model;
    protected DownloadModels downloadModelsService;
    protected Predictions predictionsService;

    protected H2OModel(T model) {
        this.model = model;
        downloadModelsService = H2OConnection.getInstance().getService(DownloadModels.class);
        predictionsService = H2OConnection.getInstance().getService(Predictions.class);
    }

    public T getModel() {
        return model;
    }


    public FileDownloadListener downloadPOJO(File path) throws IOException {
        if(!model.havePojo) {
            throw new IllegalStateException("No POJO export available for this model.  Refer to model docs provided by H2O.ai");
        }
        if(path ==  null) {
            throw new NullPointerException();
        }
        if(!path.isDirectory()) {
            throw new IllegalArgumentException("Invalid directory " + path.getPath());
        }
        String modelId = keyToString(model.modelId);
        File file = new File(path, modelId + ".java");
        if(!file.createNewFile()) {
            if (!file.canWrite()) {
                throw new IllegalArgumentException("Cannot write to file " + file.getPath());
            }
        }
        System.out.println("Downloading " + modelId + " to " + file.toString());
        Call<ResponseBody> call = downloadModelsService.fetchJavaCode(modelId);
        FileDownloadListener listener = new FileDownloadListener();
        call.enqueue(new Callback<ResponseBody>() {
            @Override
            public void onResponse(Call<ResponseBody> call, Response<ResponseBody> response) {
                if(response.isSuccessful()) {
                    writeFile(response.body(), file, listener);
                } else {
                    listener.setError("Server error: " + response.code() + " " + response.message());
                }
            }
            @Override
            public void onFailure(Call<ResponseBody> call, Throwable throwable) {
                listener.setError(throwable.toString());
            }
        });

        return listener;
    }

    public FileDownloadListener downloadMOJO(File path) throws IOException {
        if(!model.haveMojo) {
            throw new IllegalStateException("No MOJO export available for this model.  Refer to model docs provided by H2O.ai");
        }
        if(path ==  null) {
            throw new NullPointerException();
        }
        if(!path.isDirectory()) {
            throw new IllegalArgumentException("Invalid directory " + path.getPath());
        }
        String modelId = keyToString(model.modelId);
        File file = new File(path, modelId + ".zip");
        if(!file.createNewFile()) {
            if (!file.canWrite()) {
                throw new IllegalArgumentException("Cannot write to file " + file.getPath());
            }
        }
        System.out.println("Downloading " + modelId + " to " + file.toString());
        Call<ResponseBody> call = downloadModelsService.fetchMojo(modelId);
        FileDownloadListener listener = new FileDownloadListener();
        call.enqueue(new Callback<ResponseBody>() {
            @Override
            public void onResponse(Call<ResponseBody> call, Response<ResponseBody> response) {
                if(response.isSuccessful()) {
                    writeFile(response.body(), file, listener);
                } else {
                    listener.setError("Server error: " + response.code() + " " + response.message());
                }
            }
            @Override
            public void onFailure(Call<ResponseBody> call, Throwable throwable) {
                listener.setError(throwable.toString());
            }
        });

        return listener;
    }

    public H2OFrame predict(String frameId) throws H2OException {
        ModelMetricsListSchemaV3 params = new ModelMetricsListSchemaV3();
        params.frame = stringToFrameKey(frameId);
        return predict(params);
    }

    public H2OFrame predict(ModelMetricsListSchemaV3 params) throws H2OException {
        if(params == null) {
            throw new NullPointerException();
        }
        if(params.frame == null) {
            throw new IllegalArgumentException("Must specify frame to predict on");
        }
        ModelMetricsListSchemaV3 responseBody = H2OConnection.getInstance().executeWithRetries(
                predictionsService.predict(
                        keyToString(model.modelId),
                        keyToString(params.frame),
                        keyToString(params.predictionsFrame),
                        keyToString(params.deviancesFrame),
                        params.reconstructionError,
                        params.reconstructionErrorPerFeature,
                        params.deepFeaturesHiddenLayer,
                        params.deepFeaturesHiddenLayerName,
                        params.reconstructTrain,
                        params.projectArchetypes,
                        params.reverseTransform,
                        params.leafNodeAssignment,
                        params.exemplarIndex,
                        params.deviances,
                        params.customMetricFunc,
                        params._excludeFields));
        if(responseBody.deviancesFrame != null) {
            return (new H2OFrame(keyToString(responseBody.predictionsFrame))).colbind(new H2OFrame(keyToString(responseBody.deviancesFrame))).execute();
        } else {
            return new H2OFrame(keyToString(responseBody.predictionsFrame));
        }
    }

    public JobV3 predictAsync(ModelMetricsListSchemaV3 params) throws H2OException {
        if (params == null) {
            throw new NullPointerException();
        }
        if(params.frame == null) {
            throw new IllegalArgumentException("Must specify frame to predict on");
        }
        return H2OConnection.getInstance().executeWithRetries(
                predictionsService.predictAsync(
                        keyToString(model.modelId),
                        keyToString(params.frame),
                        keyToString(params.predictionsFrame),
                        keyToString(params.deviancesFrame),
                        params.reconstructionError,
                        params.reconstructionErrorPerFeature,
                        params.deepFeaturesHiddenLayer,
                        params.deepFeaturesHiddenLayerName,
                        params.reconstructTrain,
                        params.projectArchetypes,
                        params.reverseTransform,
                        params.leafNodeAssignment,
                        params.exemplarIndex,
                        params.deviances,
                        params.customMetricFunc,
                        params._excludeFields));
    }

    @Override
    public String toString() {
        return (new GsonBuilder().serializeSpecialFloatingPointValues().create()).toJson(model);
    }


}
