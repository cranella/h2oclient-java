package com.att.h2o;

import okhttp3.HttpUrl;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class H2OBoolFrameTest {

    private H2OBoolFrame frame1 = new H2OBoolFrame("testFrame1");
    private H2OBoolFrame frame2 = new H2OBoolFrame("testFrame2");

    @Test
    public void and() throws Exception {
        H2OBoolFrame newFrame = frame1.and(frame2);
        assertEquals("(& testFrame1 testFrame2)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void or() throws Exception {
        H2OBoolFrame newFrame = frame1.or(frame2);
        assertEquals("(| testFrame1 testFrame2)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void execute() throws Exception {
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
        H2OConnection conn = H2OConnection.newInstance(url);

        H2OFrame frame1 = conn.uploadFileToFrame(new File("src/test/resources/data/dataset1.csv"), "dataset1");
        H2OFrame frame2 = conn.uploadFileToFrame(new File("src/test/resources/data/dataset2.csv"), "dataset2");

        H2OFrame result1 = frame1.select(frame1.select("response").compare("==", 1.0).and(frame2.select("response").compare("==", 1.0))).execute();
        assertEquals(2440, result1.getSummary().rows);
        assertEquals(11, result1.getSummary().numColumns);
        assertEquals("c10.l68", result1.select(0,10).flatten());

        H2OFrame result2 = frame1.select(frame1.select("response").compare("==", 1.0).or(frame2.select("response").compare("==", 1.0))).execute();
        assertEquals(7378, result2.getSummary().rows);
        assertEquals(11, result2.getSummary().numColumns);
        assertEquals("c10.l95", result2.select(0,10).flatten());

        conn.removeAll();
        conn.close();
    }

}