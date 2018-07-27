package com.att.h2o;

import com.google.gson.*;
import water.bindings.pojos.*;

import java.lang.reflect.Type;

public class H2OErrorDeserializer implements JsonDeserializer<H2OErrorV3> {

    public H2OErrorDeserializer() {
    }

    public H2OErrorV3 deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
        if (json.isJsonNull()) {
            return null;
        } else {
            if (json.isJsonObject()) {
                JsonObject jobj = json.getAsJsonObject();
                if (jobj.has("__meta")) {
                    JsonObject meta = jobj.getAsJsonObject("__meta");
                    if (meta.has("schema_name")) {
                        String schema = meta.get("schema_name").getAsJsonPrimitive().getAsString();
                        if(schema.equals("H2OModelBuilderErrorV3")) {
                            return context.deserialize(jobj, H2OModelBuilderErrorV3.class);
                        } else {
                            return new Gson().fromJson(jobj , H2OErrorV3.class);
                        }
                    }
                }
            }
            throw new JsonParseException("Invalid H2OErrorV3 element " + json.toString());
        }
    }
}
