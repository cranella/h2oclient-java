package com.att.h2o;

import okhttp3.HttpUrl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import water.bindings.pojos.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

public class H2OModelTest {

    private static H2OModel<?> model;
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
        conn = H2OConnection.newInstance(url);

        H2OFrame frame = conn.uploadFileToFrame(new File("src/test/resources/data/arrhythmia.csv.gz"), "arrhythmia");
        frame.split(new float[] {0.75F}, new String[] {"train", "test"}, 56453);
        DRFParametersV3 params = new DRFParametersV3();
        params.trainingFrame = new FrameKeyV3();
        params.trainingFrame.name = "train";
        params.validationFrame = new FrameKeyV3();
        params.validationFrame.name = "test";
        params.responseColumn = new ColSpecifierV3();
        params.responseColumn.columnName = "C1";
        params.seed = 1000;

        model = (new DRFModelBuilder(params)).build();

    }

    @AfterClass
    public static void tearDown() throws Exception {
//        conn.removeAll();
        conn.close();
    }

    @Test
    public void getModel() throws Exception {
        assertTrue(model.getModel() instanceof DRFModelV3);
    }

    @Test
    public void predict_FrameId() throws Exception {
        H2OFrame predictions = model.predict("train");
        assertNotNull(predictions);
        assertNotNull(predictions.getColSummary("predict"));
        assertEquals(conn.getFrame("train").getSummary().rows, predictions.getSummary().rows);
        assertEquals("real", predictions.getColSummary("predict").type);
        assertEquals(46.1023, predictions.getColSummary("predict").mean, 0.0001);
        assertEquals(75.2600, predictions.getColSummary("predict").maxs[0], 0.0001);
        assertEquals(5.7200, predictions.getColSummary("predict").mins[0], 0.0001);
    }

    @Test
    public void predict_ModelMetricsList() throws Exception {
        ModelMetricsListSchemaV3 params = new ModelMetricsListSchemaV3();
        FrameKeyV3 frame = new FrameKeyV3();
        frame.name = "train";
        params.frame = frame;
        FrameKeyV3 predictions = new FrameKeyV3();
        predictions.name = "drf_predictions";
        params.predictionsFrame = predictions;
        params.deviances = true;
        H2OFrame predictionsAndDeviances = model.predict(params);
        assertNotNull(predictionsAndDeviances);
        assertNotNull(predictionsAndDeviances.getColSummary("predict"));
        assertNotNull(predictionsAndDeviances.getColSummary("deviance"));
        assertEquals(conn.getFrame("train").getSummary().rows, predictionsAndDeviances.getSummary().rows);
        assertEquals("real", predictionsAndDeviances.getColSummary("predict").type);
        assertEquals("real", predictionsAndDeviances.getColSummary("deviance").type);
        assertEquals(46.1023, predictionsAndDeviances.getColSummary("predict").mean, 0.0001);
        assertEquals(75.2600, predictionsAndDeviances.getColSummary("predict").maxs[0], 0.0001);
        assertEquals(5.7200, predictionsAndDeviances.getColSummary("predict").mins[0], 0.0001);
        assertEquals(20.9255, predictionsAndDeviances.getColSummary("deviance").mean, 0.0001);
    }


    //"java.lang.ClassCastException: hex.tree.drf.DRFModel cannot be cast to hex.Model$DeepFeatures" when running predictAsync

//    @Test
//    public void predictAsync() throws Exception {
//        ModelMetricsListSchemaV3 params = new ModelMetricsListSchemaV3();
//        FrameKeyV3 frame = new FrameKeyV3();
//        frame.name = "train";
//        params.frame = frame;
//        FrameKeyV3 predictions = new FrameKeyV3();
//        predictions.name = "drf_predictions_async";
//        params.predictionsFrame = predictions;
////        params.deviances = true;
//        JobV3 job = model.predictAsync(params);
//        assertNotNull(job.key);
//        assertEquals("drf_predictions_async", job.dest.name);
//
//        while(conn.poll(job.key.name).status.equals("RUNNING")) {
//            try {
//                Thread.sleep(50);
//            } catch (InterruptedException e) {
//                //No interruptions expected
//            }
//        }
//        System.out.println(conn.poll(job.key.name));
//        assertEquals("DONE",conn.poll(job.key.name).status);
//        H2OFrame predictionsAndDeviances = conn.getFrame("drf_predictions");
//        assertNotNull(predictionsAndDeviances);
//        assertNotNull(predictionsAndDeviances.getColSummary("predict"));
//        assertNotNull(predictionsAndDeviances.getColSummary("deviance"));
//        assertEquals(conn.getFrame("train").getSummary().rows, predictionsAndDeviances.getSummary().rows);
//        assertEquals("real", predictionsAndDeviances.getColSummary("predict").type);
//        assertEquals("real", predictionsAndDeviances.getColSummary("deviance").type);
//        assertEquals(0, predictionsAndDeviances.getColSummary("predict").missingCount);
//        assertEquals(0, predictionsAndDeviances.getColSummary("deviance").missingCount);
//    }

}