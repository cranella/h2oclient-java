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

public class AggregatorModelBuilderTest {

    private static AggregatorModelBuilder aggregatorModelBuilder;
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

        conn.uploadFileToFrame(new File("src/test/resources/data/dataset1.csv"), "dataset1");

        AggregatorParametersV99 params = new AggregatorParametersV99();
        params.trainingFrame = new FrameKeyV3();
        params.trainingFrame.name = "dataset1";

        aggregatorModelBuilder = new AggregatorModelBuilder(params);

    }

    @AfterClass
    public static void tearDown() throws Exception {
        conn.removeAll();
        conn.close();
    }

    @Test
    public void build() throws Exception {
        H2OModel<AggregatorModelV99> model = aggregatorModelBuilder.build();
        AggregatorModelV99 modelData = model.getModel();
        assertEquals("dataset1", modelData.dataFrame.name);
        assertEquals("aggregator", modelData.algo);
        FrameKeyV3 outputFrame = modelData.output.outputFrame;
        assertNotNull(outputFrame);
        H2OFrame aggregatedFrame = conn.getFrame(outputFrame.name);
        assertEquals(12, aggregatedFrame.getSummary().numColumns);
        assertTrue(aggregatedFrame.getSummary().rowCount < 10000);
    }

    @Test(expected = H2OModelBuilderException.class)
    public void build_InvalidParams() throws Exception {
        AggregatorParametersV99 params = new AggregatorParametersV99();
        params.trainingFrame = new FrameKeyV3();
        params.trainingFrame.name = "nonexistant";
        AggregatorModelBuilder builder = new AggregatorModelBuilder(params);
        builder.build();
    }

    @Test
    public void train() throws Exception {
        JobV3 job = aggregatorModelBuilder.train();
        assertNotNull(job.key);

        while(conn.poll(job.key.name).status.equals("RUNNING")) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
        }
        assertEquals("DONE",conn.poll(job.key.name).status);
        H2OModel<AggregatorModelV99> model = aggregatorModelBuilder.retrieveModel();
        assertNotNull(model);
        AggregatorModelV99 modelData = model.getModel();
        assertEquals(job.dest.name, modelData.modelId.name);
        assertEquals("dataset1", modelData.dataFrame.name);
        assertEquals("aggregator", modelData.algo);
        FrameKeyV3 outputFrame = modelData.output.outputFrame;
        assertNotNull(outputFrame);
        H2OFrame aggregatedFrame = conn.getFrame(outputFrame.name);
        assertEquals(12, aggregatedFrame.getSummary().numColumns);
        assertTrue(aggregatedFrame.getSummary().rowCount < 10000);
    }

    @Test(expected = H2OModelBuilderException.class)
    public void train_InvalidParams() throws Exception {
        AggregatorParametersV99 params = new AggregatorParametersV99();
        params.trainingFrame = new FrameKeyV3();
        params.trainingFrame.name = "nonexistant";
        AggregatorModelBuilder builder = new AggregatorModelBuilder(params);
        builder.train();
    }

    @Test
    public void gridSearch() throws Exception {
        Map<String,Object> hyperParams = new HashMap<>();
        hyperParams.put("k", new int[] {1,2,3,4});
        JobV3 job = aggregatorModelBuilder.gridSearch(hyperParams, null);
        assertNotNull(job.key);

        while(conn.poll(job.key.name).status.equals("RUNNING")) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
        }
        assertEquals("DONE",conn.poll(job.key.name).status);
        GridSchemaV99 gridSearchResults = aggregatorModelBuilder.retrieveGrid();
        assertNotNull(gridSearchResults);
        assertEquals(4,gridSearchResults.modelIds.length);
        assertEquals(1,gridSearchResults.hyperNames.length);
        assertEquals("k",gridSearchResults.hyperNames[0]);
        for(ModelKeyV3 key : gridSearchResults.modelIds) {
            H2OModel<AggregatorModelV99> model = conn.getModel(key.name, AggregatorModelV99.class);
            AggregatorModelV99 modelData = model.getModel();
            FrameKeyV3 outputFrame = modelData.output.outputFrame;
            assertNotNull(outputFrame);
            H2OFrame aggregatedFrame = conn.getFrame(outputFrame.name);
            assertEquals(12, aggregatedFrame.getSummary().numColumns);
            assertTrue(aggregatedFrame.getSummary().rowCount < 10000);
        }
    }

    @Test
    public void validateParams() throws Exception {
        H2OModelBuilder.ValidationResults validationResults = aggregatorModelBuilder.validateParams();
        assertEquals(0, validationResults.getErrorCount());
    }

    @Test
    public void validateParams_Invalid() throws Exception {
        AggregatorParametersV99 params = new AggregatorParametersV99();
        params.trainingFrame = new FrameKeyV3();
        params.trainingFrame.name = "nonexistant";
        AggregatorModelBuilder builder = new AggregatorModelBuilder(params);
        H2OModelBuilder.ValidationResults validationResults = builder.validateParams();
        assertTrue(validationResults.getErrorCount() > 0);
        assertEquals("ERRR",validationResults.getErrorMessages().get(0).messageType);
        assertEquals("train",validationResults.getErrorMessages().get(0).fieldName);
    }

    @Test
    public void poll() throws Exception {
        aggregatorModelBuilder.train();
        JobV3 job = aggregatorModelBuilder.poll();
        assertNotNull(job);
        while(job.status.equals("RUNNING")) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
            job = aggregatorModelBuilder.poll();
            assertNotNull(job);
        }
        assertEquals("DONE",job.status);
    }

}