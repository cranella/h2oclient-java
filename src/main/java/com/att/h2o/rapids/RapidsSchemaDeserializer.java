package com.att.h2o.rapids;

import com.google.gson.*;
import water.bindings.pojos.*;

import java.lang.reflect.Type;

public class RapidsSchemaDeserializer implements JsonDeserializer<RapidsSchemaV3> {
    public RapidsSchemaDeserializer() {
    }

    @Override
    public RapidsSchemaV3 deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
        if (json.isJsonNull()) {
            return null;
        } else {
            if (json.isJsonObject()) {
                JsonObject jsonObject = json.getAsJsonObject();
                if (jsonObject.has("__meta")) {
                    JsonObject meta = jsonObject.getAsJsonObject("__meta");
                    if (meta.has("schema_name")) {
                        String schema = meta.get("schema_name").getAsJsonPrimitive().getAsString();
                        switch (schema) {
                            case "RapidsFrameV3":
                                return context.deserialize(json, RapidsFrameV3.class);
                            case "RapidsFunctionV3":
                                return context.deserialize(json, RapidsFunctionV3.class);
                            case "RapidsNumberV3":
                                return context.deserialize(json, RapidsNumberV3.class);
                            case "RapidsNumbersV3":
                                return context.deserialize(json, RapidsNumbersV3.class);
                            case "RapidsStringV3":
                                return context.deserialize(json, RapidsStringV3.class);
                            case "RapidsStringsV3":
                                return context.deserialize(json, RapidsStringsV3.class);
                            default:
                                throw new JsonParseException("Unable to deserialize Rapids result of type " + schema);
                        }
                    }
                }
            }
            throw new JsonParseException("Invalid RapidsSchemaV3 JSON " + json.toString());
        }
    }
}
