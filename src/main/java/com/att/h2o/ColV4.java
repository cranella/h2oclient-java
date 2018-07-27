package com.att.h2o;

import com.google.gson.GsonBuilder;
import water.bindings.pojos.ColV3;

public class ColV4 extends ColV3 {

    @Override
    public String toString() {
        return (new GsonBuilder().serializeSpecialFloatingPointValues().create()).toJson(this);
    }
}
