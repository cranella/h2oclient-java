package com.att.h2o.examples;

import com.att.h2o.*;
import okhttp3.HttpUrl;
import okhttp3.logging.HttpLoggingInterceptor;
import water.bindings.pojos.GBMModelV3;

import java.io.File;

public class GBM_Example {

    public static void main(String[] args) {

        HttpUrl url = new HttpUrl.Builder().scheme("http").host("localhost").port(54321).build();
        H2OConnection.setHttpLogLevel(HttpLoggingInterceptor.Level.BODY);
        H2OConnection.setHttpLogger(System.out::println);
        try(H2OConnection conn = H2OConnection.newInstance(url)) {
            System.out.println("Importing dataset");
            H2OFrame data = conn.uploadFileToFrame(new File("src/test/resources/data/arrhythmia.csv.gz"),"arrhythmia.hex");
            H2OFrame[] splits = data.split(new float[] {0.75F}, new String[] {"train","test"}, 906317);
            System.out.println("Training GBM");
            H2OModel<GBMModelV3> model = (new GBMModelBuilder("train","test","C1")).build();
            System.out.println("GBM build done");
            System.out.println("New GBM model: " + model);
            H2OFrame predictions = model.predict("train");
            System.out.println(predictions);
        } catch (H2OException e) {
            e.printStackTrace();
        }
    }
}
