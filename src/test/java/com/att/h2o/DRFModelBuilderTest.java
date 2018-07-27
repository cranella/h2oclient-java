package com.att.h2o;

import okhttp3.HttpUrl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import water.bindings.pojos.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class DRFModelBuilderTest {

    private static DRFModelBuilder drfModelBuilder;
    private static H2OConnection conn;

    @BeforeClass
    public static void setUp() throws Exception {
        Properties props = new Properties();
        try (InputStream input = H2OConnection.class.getClassLoader().getResourceAsStream("test.properties")) {
            props.load(input);
        } catch (IOException e) {
            System.err.println("Error loading test.properties file");
            throw e;
        }

        String host = props.getProperty("h2o.host");
        int port = Integer.parseInt(props.getProperty("h2o.port"));
        HttpUrl url = new HttpUrl.Builder().scheme("http").host(host).port(port).build();
//        H2OConnection.setHttpLogLevel(HttpLoggingInterceptor.Level.BASIC);
        conn = H2OConnection.newInstance(url);

        H2OFrame frame = conn.uploadFileToFrame(new File("src/test/resources/data/dataset1.csv"), "dataset1");
        frame.split(new float[] {0.75F}, new String[] {"train", "test"}, 56453);

        DRFParametersV3 params = new DRFParametersV3();
        params.trainingFrame = new FrameKeyV3();
        params.trainingFrame.name = "train";
        params.validationFrame = new FrameKeyV3();
        params.validationFrame.name = "test";
        params.responseColumn = new ColSpecifierV3();
        params.responseColumn.columnName = "response";
        params.seed = 1000;

        drfModelBuilder = new DRFModelBuilder(params);

    }

    @AfterClass
    public static void tearDown() throws Exception {
        conn.removeAll();
        conn.close();
    }

    @Test
    public void build() throws Exception {
        H2OModel<DRFModelV3> model = drfModelBuilder.build();
        DRFModelV3 modelData = model.getModel();
        assertEquals("train", modelData.dataFrame.name);
        assertEquals("response", modelData.responseColumnName);
        assertEquals("drf", modelData.algo);
        ModelMetricsBaseV3 validationMetrics = modelData.output.validationMetrics;
        ModelMetricsBaseV3 trainingMetrics = modelData.output.trainingMetrics;
        assertEquals("test", validationMetrics.frame.name);
        assertEquals(0.262225, validationMetrics.mse,0.000001);
        assertEquals(0.270706,trainingMetrics.mse,0.000001);
    }

    @Test(expected = H2OModelBuilderException.class)
    public void build_InvalidParams() throws Exception {
        DRFParametersV3 params = new DRFParametersV3();
        params.trainingFrame = new FrameKeyV3();
        params.trainingFrame.name = "nonexistant";
        DRFModelBuilder builder = new DRFModelBuilder(params);
        builder.build();
    }

    @Test
    public void train() throws Exception {
        JobV3 job = drfModelBuilder.train();
        assertNotNull(job.key);

        while(conn.poll(job.key.name).status.equals("RUNNING")) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
        }
        assertEquals("DONE",conn.poll(job.key.name).status);
        H2OModel<DRFModelV3> model = drfModelBuilder.retrieveModel();
        assertNotNull(model);
        DRFModelV3 modelData = model.getModel();
        assertEquals(job.dest.name, modelData.modelId.name);
        assertEquals("train", modelData.dataFrame.name);
        assertEquals("response", modelData.responseColumnName);
        assertEquals("drf", modelData.algo);
        ModelMetricsBaseV3 validationMetrics = modelData.output.validationMetrics;
        ModelMetricsBaseV3 trainingMetrics = modelData.output.trainingMetrics;
        assertEquals("test", validationMetrics.frame.name);
        assertEquals(0.262225, validationMetrics.mse,0.000001);
        assertEquals(0.270706,trainingMetrics.mse,0.000001);
    }

    @Test(expected = H2OModelBuilderException.class)
    public void train_InvalidParams() throws Exception {
        DRFParametersV3 params = new DRFParametersV3();
        params.trainingFrame = new FrameKeyV3();
        params.trainingFrame.name = "nonexistant";
        DRFModelBuilder builder = new DRFModelBuilder(params);
        builder.train();
    }

    @Test
    public void gridSearch() throws Exception {
        Map<String,Object> hyperParams = new HashMap<>();
        hyperParams.put("mtries", new int[] {3,5,10});
        JobV3 job = drfModelBuilder.gridSearch(hyperParams, null);
        assertNotNull(job.key);

        while(conn.poll(job.key.name).status.equals("RUNNING")) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
        }
        assertEquals("DONE",conn.poll(job.key.name).status);
        GridSchemaV99 gridSearchResults = drfModelBuilder.retrieveGrid();
        assertNotNull(gridSearchResults);
        assertEquals(3,gridSearchResults.modelIds.length);
        assertEquals(1,gridSearchResults.hyperNames.length);
        assertEquals("mtries",gridSearchResults.hyperNames[0]);
        for(ModelKeyV3 key : gridSearchResults.modelIds) {
            H2OModel<DRFModelV3> model = conn.getModel(key.name, DRFModelV3.class);
            DRFModelV3 modelData = model.getModel();
            assertEquals("train", modelData.dataFrame.name);
            assertEquals("response", modelData.responseColumnName);
            assertEquals("drf", modelData.algo);
            ModelMetricsBaseV3 metrics = modelData.output.validationMetrics;
            assertEquals("test", metrics.frame.name);
            assertTrue(metrics.mse > 0);
        }
    }

    @Test
    public void retrieveGrid() throws Exception {
        Map<String,Object> hyperParams = new HashMap<>();
        hyperParams.put("mtries", new int[] {3,5,10});
        JobV3 job = drfModelBuilder.gridSearch(hyperParams, null);
        assertNotNull(job.key);

        while(conn.poll(job.key.name).status.equals("RUNNING")) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
        }
        assertEquals("DONE",conn.poll(job.key.name).status);
        GridSchemaV99 gridSearchResults = drfModelBuilder.retrieveGrid("mse",true);
        assertNotNull(gridSearchResults);
        assertEquals(3,gridSearchResults.modelIds.length);
        assertEquals(1,gridSearchResults.hyperNames.length);
        assertEquals("mtries",gridSearchResults.hyperNames[0]);
        double prevMse = Double.MAX_VALUE;
        for(ModelKeyV3 key : gridSearchResults.modelIds) {
            H2OModel<DRFModelV3> model = conn.getModel(key.name, DRFModelV3.class);
            DRFModelV3 modelData = model.getModel();
            assertEquals("train", modelData.dataFrame.name);
            assertEquals("response", modelData.responseColumnName);
            assertEquals("drf", modelData.algo);
            ModelMetricsBaseV3 metrics = modelData.output.validationMetrics;
            assertEquals("test", metrics.frame.name);
            assertTrue(metrics.mse < prevMse);
            prevMse = metrics.mse;
        }
    }

    @Test
    public void validateParams() throws Exception {
        H2OModelBuilder.ValidationResults validationResults = drfModelBuilder.validateParams();
        assertEquals(0, validationResults.getErrorCount());
    }

    @Test
    public void validateParams_Invalid() throws Exception {
        DRFParametersV3 params = new DRFParametersV3();
        params.trainingFrame = new FrameKeyV3();
        params.trainingFrame.name = "nonexistant";
        DRFModelBuilder builder = new DRFModelBuilder(params);
        H2OModelBuilder.ValidationResults validationResults = builder.validateParams();
        assertTrue(validationResults.getErrorCount() > 0);
        assertEquals("ERRR",validationResults.getErrorMessages().get(0).messageType);
        assertEquals("train",validationResults.getErrorMessages().get(0).fieldName);

    }

    @Test
    public void poll() throws Exception {
        drfModelBuilder.train();
        JobV3 job = drfModelBuilder.poll();
        assertNotNull(job);
        while(job.status.equals("RUNNING")) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
            job = drfModelBuilder.poll();
            assertNotNull(job);
        }
        assertEquals("DONE",job.status);
    }

}