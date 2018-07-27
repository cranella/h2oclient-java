package com.att.h2o;

import okhttp3.HttpUrl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class H2OGroupByTest {

    private static H2OConnection conn;
    private static H2OFrame frame;

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

        frame = conn.uploadFileToFrame(new File("src/test/resources/data/dataset1.csv"), "dataset1");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        conn.removeAll();
        conn.close();
    }

    @Test
    public void min() throws Exception {
        H2OGroupBy groupBy = frame.groupBy(new String[] {"C10"}).min(new String[] {"C5"}, "rm");
        assertEquals("(GB dataset1 [10] 'min' 5 'rm')", groupBy.getRapidsBuilder().getAst().toString());
    }


    @Test
    public void max() throws Exception {
        H2OGroupBy groupBy = frame.groupBy(new String[] {"C10"}).max(new String[] {"C5"}, "rm");
        assertEquals("(GB dataset1 [10] 'max' 5 'rm')", groupBy.getRapidsBuilder().getAst().toString());
    }


    @Test
    public void mean() throws Exception {
        H2OGroupBy groupBy = frame.groupBy(new String[] {"C10"}).mean(new String[] {"C5"}, "rm");
        assertEquals("(GB dataset1 [10] 'mean' 5 'rm')", groupBy.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void count() throws Exception {
        H2OGroupBy groupBy = frame.groupBy(new String[] {"C10"}).count("rm");
        assertEquals("(GB dataset1 [10] 'nrow' 0 'rm')", groupBy.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void sum() throws Exception {
        H2OGroupBy groupBy = frame.groupBy(new String[] {"C10"}).sum(new String[] {"C5"}, "rm");
        assertEquals("(GB dataset1 [10] 'sum' 5 'rm')", groupBy.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void stdDev() throws Exception {
        H2OGroupBy groupBy = frame.groupBy(new String[] {"C10"}).stdDev(new String[] {"C5"}, "rm");
        assertEquals("(GB dataset1 [10] 'sdev' 5 'rm')", groupBy.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void var() throws Exception {
        H2OGroupBy groupBy = frame.groupBy(new String[] {"C10"}).var(new String[] {"C5"}, "rm");
        assertEquals("(GB dataset1 [10] 'var' 5 'rm')", groupBy.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void sumSquares() throws Exception {
        H2OGroupBy groupBy = frame.groupBy(new String[] {"C10"}).sumSquares(new String[] {"C5"}, "rm");
        assertEquals("(GB dataset1 [10] 'sumSquares' 5 'rm')", groupBy.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void mode() throws Exception {
        H2OGroupBy groupBy = frame.groupBy(new String[] {"C10"}).mode(new String[] {"C8"}, "rm");
        assertEquals("(GB dataset1 [10] 'mode' 8 'rm')", groupBy.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void getFrame() throws Exception {
        H2OFrame results1 = frame.groupBy(new String[] {"C10"}).min(new String[] {"C5"}, "rm").getFrame();
        assertEquals(-100.0, results1.select(1,1).flatten());

        H2OFrame results2 = frame.groupBy(new String[] {"C10"}).max(new String[] {"C5"}, "rm").getFrame();
        assertEquals(96.0, results2.select(1,1).flatten());

        H2OFrame results3 = frame.groupBy(new String[] {"C10"}).mean(new String[] {"C5"}, "rm").getFrame();
        assertEquals(-0.94382, (Double) results3.select(1,1).flatten(), 0.00001);

        H2OFrame results4 = frame.groupBy(new String[] {"C10"}).count("rm").getFrame();
        assertEquals(90.0, results4.select(1,1).flatten());

        H2OFrame results5 = frame.groupBy(new String[] {"C10"}).sum(new String[] {"C5"}, "rm").getFrame();
        assertEquals(-84.0, results5.select(1,1).flatten());

        H2OFrame results6 = frame.groupBy(new String[] {"C10"}).stdDev(new String[] {"C5"}, "rm").getFrame();
        assertEquals(58.5907, (Double) results6.select(1,1).flatten(), 0.0001);

        H2OFrame results7 = frame.groupBy(new String[] {"C10"}).var(new String[] {"C5"}, "rm").getFrame();
        assertEquals(3432.87, (Double) results7.select(1,1).flatten(), 0.01);

        H2OFrame results8 = frame.groupBy(new String[] {"C10"}).sumSquares(new String[] {"C5"}, "rm").getFrame();
        assertEquals(302172.0, results8.select(1,1).flatten());

        H2OFrame results9 = frame.groupBy(new String[] {"C10"}).mode(new String[] {"C8"}, "rm").getFrame();
        assertEquals(18.0, results9.select(1,1).flatten());
    }

    @Test
    public void getFrame_assign() throws Exception {
        frame.groupBy(new String[] {"C10"}).min(new String[] {"C5"}, "rm").getFrame("groupByAssign");
        H2OFrame results = conn.getFrame("groupByAssign");
        assertNotNull(results);
        assertEquals(-100.0, results.select(1,1).flatten());
    }

}