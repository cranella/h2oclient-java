package com.att.h2o;

/* Copied from decompiled water.bindings.H2oApi in h2o-bindings v3.16.0.2
    Modified 6/20/18 to deserialize ModelParameters and ModelOutput based on schema type
 */

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import water.bindings.pojos.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Iterator;

public class H2OProvidedTypeAdapters {

    protected H2OProvidedTypeAdapters() {
    }

    public static class ModelV3TypeAdapter implements TypeAdapterFactory {
        public ModelV3TypeAdapter() {
        }

        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            final Class<? super T> rawType = type.getRawType();
            if (!ModelBuilderSchema.class.isAssignableFrom(rawType) && !ModelSchemaBaseV3.class.isAssignableFrom(rawType)) {
                return null;
            } else {
                final TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);
                return new TypeAdapter<T>() {
                    public void write(JsonWriter out, T value) throws IOException {
                        delegate.write(out, value);
                    }

                    public T read(JsonReader in) throws IOException {
                        JsonObject jobj = (new JsonParser()).parse(in).getAsJsonObject();
                        if (jobj.has("parameters") && jobj.get("parameters").isJsonArray()) {
                            JsonArray jarr = jobj.get("parameters").getAsJsonArray();
                            JsonObject paramsNew = new JsonObject();
                            Iterator i$ = jarr.iterator();

                            while(i$.hasNext()) {
                                JsonElement item = (JsonElement)i$.next();
                                JsonObject itemObj = item.getAsJsonObject();
                                paramsNew.add(itemObj.get("name").getAsString(), itemObj.get("actual_value"));
                            }

                            jobj.add("parameters", paramsNew);
                        }

                        return (T) (new Gson()).fromJson(jobj, rawType);
                    }
                };
            }
        }
    }

    public static class ModelParametersDeserializer implements JsonDeserializer<ModelParametersSchemaV3> {
        public ModelParametersDeserializer() {
        }

        public ModelParametersSchemaV3 deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (json.isJsonNull()) {
                return null;
            } else {
                if (json.isJsonObject()) {
                    JsonObject jobj = json.getAsJsonObject();
                    if (jobj.has("__meta")) {
                        JsonObject meta = jobj.getAsJsonObject("__meta");
                        if (meta.has("schema_type")) {
                            String schema = meta.get("schema_type").getAsJsonPrimitive().getAsString();
                            switch (schema) {
                                case "XGBoostParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, XGBoostParametersV3.class);
                                case "DeepWaterParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, DeepWaterParametersV3.class);
                                case "KMeansParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, KMeansParametersV3.class);
                                case "NaiveBayesParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, NaiveBayesParametersV3.class);
                                case "DRFParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, DRFParametersV3.class);
                                case "GBMParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, GBMParametersV3.class);
                                case "GLMParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, GLMParametersV3.class);
                                case "PCAParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, PCAParametersV3.class);
                                case "SVDParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, SVDParametersV99.class);
                                case "GLRMParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, GLRMParametersV3.class);
                                case "Word2VecParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, Word2VecParametersV3.class);
                                case "StackedEnsembleParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, StackedEnsembleParametersV99.class);
                                case "AggregatorParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, AggregatorParametersV99.class);
                                case "DeepLearningParameters":
                                    return (ModelParametersSchemaV3) context.deserialize(json, DeepLearningParametersV3.class);
                                default:
                                    throw new JsonParseException("Unable to deserialize model parameters of type " + schema);
                            }
                        }
                    }
                }
                throw new JsonParseException("Invalid ModelParametersSchemaV3 element " + json.toString());
            }
        }
    }

    public static class ModelOutputDeserializer implements JsonDeserializer<ModelOutputSchemaV3> {
        public ModelOutputDeserializer() {
        }

        public ModelOutputSchemaV3 deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (json.isJsonNull()) {
                return null;
            } else {
                if (json.isJsonObject()) {
                    JsonObject jobj = json.getAsJsonObject();
                    if (jobj.has("__meta")) {
                        JsonObject meta = jobj.getAsJsonObject("__meta");
                        if (meta.has("schema_type")) {
                            String schema = meta.get("schema_type").getAsJsonPrimitive().getAsString();
                            switch (schema) {
                                case "XGBoostOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, XGBoostModelOutputV3.class);
                                case "DeepWaterOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, DeepWaterModelOutputV3.class);
                                case "KMeansOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, KMeansModelOutputV3.class);
                                case "NaiveBayesOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, NaiveBayesModelOutputV3.class);
                                case "DRFOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, DRFModelOutputV3.class);
                                case "GBMOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, GBMModelOutputV3.class);
                                case "GLMOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, GLMModelOutputV3.class);
                                case "PCAOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, PCAModelOutputV3.class);
                                case "SVDOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, SVDModelOutputV99.class);
                                case "GLRMOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, GLRMModelOutputV3.class);
                                case "Word2VecOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, Word2VecModelOutputV3.class);
                                case "StackedEnsembleOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, StackedEnsembleModelOutputV99.class);
                                case "AggregatorOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, AggregatorModelOutputV99.class);
                                case "DeepLearningOutput":
                                    return (ModelOutputSchemaV3) context.deserialize(json, DeepLearningModelOutputV3.class);
                                default:
                                    throw new JsonParseException("Unable to deserialize model output of type " + schema);
                            }
                        }
                    }
                }
                throw new JsonParseException("Invalid ModelOutputSchemaV3 element " + json.toString());
            }
        }
    }

    public static class ModelSchemaDeserializer implements JsonDeserializer<ModelSchemaBaseV3> {
        public ModelSchemaDeserializer() {
        }

        public ModelSchemaBaseV3 deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (json.isJsonNull()) {
                return null;
            } else {
                if (json.isJsonObject()) {
                    JsonObject jobj = json.getAsJsonObject();
                    if (jobj.has("algo")) {
                        String algo = jobj.get("algo").getAsJsonPrimitive().getAsString().toLowerCase();
                        switch(algo) {
                            case "xgboost":
                                return (ModelSchemaBaseV3)context.deserialize(json, XGBoostModelV3.class);
                            case "deepwater":
                                return (ModelSchemaBaseV3)context.deserialize(json, DeepWaterModelV3.class);
                            case "kmeans":
                                return (ModelSchemaBaseV3)context.deserialize(json, KMeansModelV3.class);
                            case "naivebayes":
                                return (ModelSchemaBaseV3)context.deserialize(json, NaiveBayesModelV3.class);
                            case "drf":
                                return (ModelSchemaBaseV3)context.deserialize(json, DRFModelV3.class);
                            case "gbm":
                                return (ModelSchemaBaseV3)context.deserialize(json, GBMModelV3.class);
                            case "glm":
                                return (ModelSchemaBaseV3)context.deserialize(json, GLMModelV3.class);
                            case "pca":
                                return (ModelSchemaBaseV3)context.deserialize(json, PCAModelV3.class);
                            case "svd":
                                return (ModelSchemaBaseV3)context.deserialize(json, SVDModelV99.class);
                            case "glrm":
                                return (ModelSchemaBaseV3)context.deserialize(json, GLRMModelV3.class);
                            case "word2vec":
                                return (ModelSchemaBaseV3)context.deserialize(json, Word2VecModelV3.class);
                            case "stackedensemble":
                                return (ModelSchemaBaseV3)context.deserialize(json, StackedEnsembleModelV99.class);
                            case "aggregator":
                                return (ModelSchemaBaseV3)context.deserialize(json, AggregatorModelV99.class);
                            case "deeplearning":
                                return (ModelSchemaBaseV3)context.deserialize(json, DeepLearningModelV3.class);
                            default:
                                throw new JsonParseException("Unable to deserialize model of type " + algo);
                        }
                    }
                }

                throw new JsonParseException("Invalid ModelSchemaBaseV3 element " + json.toString());
            }
        }
    }

    public static class ModelDeserializer implements JsonDeserializer<ModelBuilderSchema> {
        public ModelDeserializer() {
        }

        public ModelBuilderSchema deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            if (json.isJsonNull()) {
                return null;
            } else {
                if (json.isJsonObject()) {
                    JsonObject jobj = json.getAsJsonObject();
                    if (jobj.has("algo")) {
                        String algo = jobj.get("algo").getAsJsonPrimitive().getAsString().toLowerCase();
                        switch(algo) {
                            case "xgboost":
                                return (ModelBuilderSchema)context.deserialize(json, XGBoostV3.class);
                            case "deepwater":
                                return (ModelBuilderSchema)context.deserialize(json, DeepWaterV3.class);
                            case "kmeans":
                                return (ModelBuilderSchema)context.deserialize(json, KMeansV3.class);
                            case "naivebayes":
                                return (ModelBuilderSchema)context.deserialize(json, NaiveBayesV3.class);
                            case "drf":
                                return (ModelBuilderSchema)context.deserialize(json, DRFV3.class);
                            case "gbm":
                                return (ModelBuilderSchema)context.deserialize(json, GBMV3.class);
                            case "glm":
                                return (ModelBuilderSchema)context.deserialize(json, GLMV3.class);
                            case "pca":
                                return (ModelBuilderSchema)context.deserialize(json, PCAV3.class);
                            case "svd":
                                return (ModelBuilderSchema)context.deserialize(json, SVDV99.class);
                            case "glrm":
                                return (ModelBuilderSchema)context.deserialize(json, GLRMV3.class);
                            case "word2vec":
                                return (ModelBuilderSchema)context.deserialize(json, Word2VecV3.class);
                            case "stackedensemble":
                                return (ModelBuilderSchema)context.deserialize(json, StackedEnsembleV99.class);
                            case "aggregator":
                                return (ModelBuilderSchema)context.deserialize(json, AggregatorV99.class);
                            case "deeplearning":
                                return (ModelBuilderSchema)context.deserialize(json, DeepLearningV3.class);
                            default:
                                throw new JsonParseException("Unable to deserialize model builder of type " + algo);
                        }
                    }
                }

                throw new JsonParseException("Invalid ModelBuilderSchema element " + json.toString());
            }
        }
    }

    public static class ColSerializer implements JsonSerializer<ColSpecifierV3> {
        public ColSerializer() {
        }

        public JsonElement serialize(ColSpecifierV3 col, Type typeOfCol, JsonSerializationContext context) {
            return new JsonPrimitive(col.columnName);
        }
    }

    public static class KeySerializer implements JsonSerializer<KeyV3>, JsonDeserializer<KeyV3> {
        public KeySerializer() {
        }

        public JsonElement serialize(KeyV3 key, Type typeOfKey, JsonSerializationContext context) {
            return new JsonPrimitive(key.name);
        }

        public KeyV3 deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
            if (json.isJsonNull()) {
                return null;
            } else {
                JsonObject jobj = json.getAsJsonObject();
                String type = jobj.get("type").getAsString();
                switch(type) {
                    case "Key<Grid>":
                        return (KeyV3)context.deserialize(jobj, GridKeyV3.class);
                    case "Key<Frame>":
                        return (KeyV3)context.deserialize(jobj, FrameKeyV3.class);
                    case "Key<Model>":
                        return (KeyV3)context.deserialize(jobj, ModelKeyV3.class);
                    case "Key<Job>":
                        return (KeyV3)context.deserialize(jobj, JobKeyV3.class);
                    case "Key<Keyed>":
                        return new Gson().fromJson(jobj , KeyV3.class);
                    default:
                        throw new JsonParseException("Unable to deserialize key of type " + type);
                }
            }
        }
    }
}
