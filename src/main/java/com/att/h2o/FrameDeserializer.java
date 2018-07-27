package com.att.h2o;

import com.google.gson.*;

import java.lang.reflect.Type;

public class FrameDeserializer implements JsonDeserializer<FrameV4> {

    public FrameDeserializer() {}

    public FrameV4 deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        if (json.isJsonNull()) {
            return null;
        } else {
            if (json.isJsonObject()) {
                JsonObject jsonObject = json.getAsJsonObject();
                return new Gson().fromJson(jsonObject.get("frames").getAsJsonArray().get(0), FrameV4.class);

            }
            throw new JsonParseException("Invalid JSON response body");
        }
    }
}
