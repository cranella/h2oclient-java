package com.att.h2o;

import com.att.h2o.rapids.ArrayIndices;
import okhttp3.HttpUrl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import water.bindings.pojos.*;
import water.bindings.proxies.retrofit.Frames;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;

import static org.junit.Assert.*;

public class H2OConnectionTest {

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
    }

    @AfterClass
    public static void tearDown() throws Exception {
        conn.removeAll();
        conn.close();
    }

    @Test
    public void getStatus() throws Exception {
        assertNotNull(conn.getStatus());
    }

    @Test
    public void isValid() throws Exception {
        assertTrue(conn.isValid());
    }

    @Test
    public void poll() throws Exception {
        File file = new File("src/test/resources/data/dataset1.csv");
        H2OFrame frame = conn.uploadFileToFrame(file);
        JobV3 job = (new DRFModelBuilder(frame.getFrameId(),null,"response")).train();
        JobV3 jobPoll =  conn.poll(job.key.name);
        assertNotNull(jobPoll);
        assertEquals(job.key.name, jobPoll.key.name);
    }

    @Test
    public void cancelJob() throws Exception {
        File file = new File("src/test/resources/data/dataset1.csv");
        H2OFrame frame = conn.uploadFileToFrame(file);
        JobV3 job = (new DRFModelBuilder(frame.getFrameId(),null,"response")).train();
        conn.cancelJob(job.key.name);
        while(conn.poll(job.key.name).status.equals("CANCEL_PENDING")) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
        }
        assertEquals("CANCELLED", conn.poll(job.key.name).status);
    }

    @Test
    public void waitForJobCompletion() throws Exception {
        File file = new File("src/test/resources/data/dataset1.csv");
        H2OFrame frame = conn.uploadFileToFrame(file);
        JobV3 jobStart = (new DRFModelBuilder(frame.getFrameId(),null,"response")).train();
        JobV3 jobEnd = conn.waitForJobCompletion(jobStart.key.name);
        assertEquals("DONE", jobEnd.status);
        assertEquals(1.0, jobEnd.progress, 0);
    }

    @Test
    public void uploadFileToFrame() throws Exception {
        conn.remove("arrhythmia");
        File file = new File("src/test/resources/data/arrhythmia.csv.gz");
        H2OFrame frame = conn.uploadFileToFrame(file, "arrhythmia");
        assertEquals("arrhythmia", frame.getFrameId());
        assertEquals(280, frame.getSummary().numColumns);
        assertEquals(452, frame.getSummary().rows);
        assertEquals(75.0, frame.select(new ArrayIndices(0), new ArrayIndices(0)).flatten());
    }

    @Test
    public void importFileToFrame() throws Exception {
        conn.remove("arrhythmia");
        URL url = this.getClass().getResource("../../../data/arrhythmia.csv.gz");
        H2OFrame frame = conn.importFileToFrame(url.getFile(), "arrhythmia");
        assertEquals("arrhythmia", frame.getFrameId());
        assertEquals(280, frame.getSummary().numColumns);
        assertEquals(452, frame.getSummary().rows);
        assertEquals(75.0, frame.select(new ArrayIndices(0), new ArrayIndices(0)).flatten());
    }

    @Test
    public void importRemoteFile() throws Exception {
        URL url = this.getClass().getResource("../../../data/arrhythmia.csv.gz");
        String frameId = conn.importRemoteFile(url.getFile());
        assertNotNull(frameId);
        assertNotNull(conn.getFrame(frameId));
    }

    @Test
    public void importRemoteFiles() throws Exception {
        URL url = this.getClass().getResource("../../../data/");
        String[] frameIds = conn.importRemoteFiles(url.getPath(), ".*");
        assertEquals(4, frameIds.length);
        assertNotNull(conn.getFrame(frameIds[0]));
        assertNotNull(conn.getFrame(frameIds[1]));
        assertNotNull(conn.getFrame(frameIds[2]));
        assertNotNull(conn.getFrame(frameIds[3]));
    }

    @Test
    public void upload() throws Exception {
        File file = new File("src/test/resources/data/arrhythmia.csv.gz");
        String frameId = conn.upload(file, false);
        assertNotNull(frameId);
        assertNotNull(conn.getFrame(frameId));
    }

    @Test
    public void parseSetup() throws Exception {
        File file = new File("src/test/resources/data/arrhythmia.csv.gz");
        String frameId = conn.upload(file, false);
        String[] sourceFrames = new String[] {frameId};
        ParseV3 params = conn.parseSetup(sourceFrames);
        assertNotNull(params);
        assertEquals(frameId, params.sourceFrames[0].name);
        assertEquals("Key_Frame__" + frameId + ".hex", params.destinationFrame.name);
        assertEquals(ApiParseTypeValuesProvider.CSV, params.parseType);
        assertEquals(280, params.numberColumns);
    }

    @Test
    public void parse() throws Exception {
        File file1 = new File("src/test/resources/data/arrhythmia.csv.gz");
        String frameId1 = conn.upload(file1, false);
        String[] sourceFrames1 = new String[] {frameId1};
        ParseV3 params1 = conn.parseSetup(sourceFrames1);
        FrameKeyV3 destination1 = new FrameKeyV3();
        destination1.name = "arrhythmia";
        params1.destinationFrame = destination1;
        H2OFrame frame1 = conn.parse(params1);
        assertNotNull(frame1);
        assertEquals("arrhythmia", frame1.getFrameId());
        assertEquals(280, frame1.getSummary().numColumns);
        assertEquals(452, frame1.getSummary().rows);
        HashSet<String> columnNames1 = new HashSet<>();
        for(ColV3 col : frame1.getSummary().columns) {
            columnNames1.add(col.label);
        }
        assertTrue(columnNames1.contains("C1"));
        assertTrue(columnNames1.contains("C2"));
        assertTrue(columnNames1.contains("C3"));
        assertTrue(columnNames1.contains("C4"));
        assertTrue(columnNames1.contains("C5"));
        assertEquals(46.4712, frame1.getColSummary("C1").mean, 0.0001);
        assertEquals(20.5417, frame1.getColSummary("C18").sigma, 0.0001);
        assertEquals("int",frame1.getColSummary("C1").type);

        File file2 = new File("src/test/resources/data/dataset1.csv");
        String frameId2 = conn.upload(file2, false);
        String[] sourceFrames2 = new String[] {frameId2};
        ParseV3 params2 = conn.parseSetup(sourceFrames2);
        FrameKeyV3 destination2 = new FrameKeyV3();
        destination2.name = "another_csv_dataset";
        params2.destinationFrame = destination2;
        H2OFrame frame2 = conn.parse(params2);
        assertNotNull(frame2);
        assertEquals("another_csv_dataset", frame2.getFrameId());
        assertEquals(11, frame2.getSummary().numColumns);
        assertEquals(10000, frame2.getSummary().rows);
        HashSet<String> columnNames2 = new HashSet<>();
        for(ColV3 col : frame2.getSummary().columns) {
            columnNames2.add(col.label);
        }
        assertTrue(columnNames2.contains("response"));
        assertTrue(columnNames2.contains("C10"));
        assertTrue(columnNames2.contains("C9"));
        assertTrue(columnNames2.contains("C8"));
        assertTrue(columnNames2.contains("C7"));
        assertEquals(0.4465, frame2.getColSummary("C1").mean, 0.0001);
        assertEquals(57.5855, frame2.getColSummary("C2").sigma, 0.0001);
        assertEquals("int",frame2.getColSummary("C5").type);
    }

    @Test
    public void parseAsync() throws Exception {
        File file = new File("src/test/resources/data/dataset2.csv");
        String frameId = conn.upload(file, false);
        String[] sourceFrames = new String[] {frameId};
        ParseV3 params = conn.parseSetup(sourceFrames);
        FrameKeyV3 destination = new FrameKeyV3();
        destination.name = "another_csv_dataset";
        params.destinationFrame = destination;
        JobV3 job = conn.parseAsync(params);
        assertNotNull(job.key);
        assertEquals("another_csv_dataset", job.dest.name);

        while(conn.poll(job.key.name).status.equals("RUNNING")) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                //No interruptions expected
            }
        }
        assertEquals("DONE",conn.poll(job.key.name).status);

        H2OFrame frame = conn.getFrame("another_csv_dataset");
        assertNotNull(frame);
        assertEquals(11, frame.getSummary().numColumns);
        assertEquals(10000, frame.getSummary().rows);
        HashSet<String> columnNames = new HashSet<>();
        for(ColV3 col : frame.getSummary().columns) {
            columnNames.add(col.label);
        }
        assertTrue(columnNames.contains("response"));
        assertTrue(columnNames.contains("C10"));
        assertTrue(columnNames.contains("C9"));
        assertTrue(columnNames.contains("C8"));
        assertTrue(columnNames.contains("C7"));
        assertEquals(0.0171, frame.getColSummary("C1").mean, 0.0001);
        assertEquals(58.1112,frame.getColSummary("C5").sigma, 0.0001);
        assertEquals("enum", frame.getColSummary("C2").type);
    }

    @Test
    public void getFrame() throws Exception {
        File file = new File("src/test/resources/data/arrhythmia.csv.gz");
        conn.uploadFileToFrame(file, "arrhythmia");
        H2OFrame frame = conn.getFrame("arrhythmia");
        assertNotNull(frame);
        assertEquals("arrhythmia", frame.getFrameId());
    }

    @Test
    public void getModel() throws Exception {
        File file = new File("src/test/resources/data/dataset1.csv");
        H2OFrame frame = conn.uploadFileToFrame(file);
        JobV3 job = (new DRFModelBuilder(frame.getFrameId(),null,"response")).train();
        conn.waitForJobCompletion(job.key.name);
        H2OModel<DRFModelV3> model = conn.getModel(job.dest.name, DRFModelV3.class);
        assertNotNull(model);
        assertEquals(job.dest.name, model.getModel().modelId.name);
    }

    @Test
    public void remove() throws Exception {
        File file = new File("src/test/resources/data/arrhythmia.csv.gz");
        String frameId = conn.upload(file, false);
        conn.remove(frameId);
        try {
            conn.getFrame(frameId);
        } catch (H2OException e) {
            assertEquals(404, e.getHttpStatus());
        }
    }

    @Test
    public void removeAll() throws Exception {
        conn.upload(new File("src/test/resources/data/arrhythmia.csv.gz"), false);
        conn.upload(new File("src/test/resources/data/dataset1.csv"), false);
        conn.upload(new File("src/test/resources/data/dataset2.csv"), false);
        conn.upload(new File("src/test/resources/data/dataset3.csv"), false);
        conn.removeAll();

        FramesV3 list = conn.getService(Frames.class).list().execute().body();
        assertEquals(0, list.frames.length);
    }

    @Test
    public void evaluateRapidsExpression() throws Exception {
        File file = new File("src/test/resources/data/dataset1.csv");
        H2OFrame frame = conn.uploadFileToFrame(file, "dataset");
        RapidsFrameV3 frameResult = (RapidsFrameV3) conn.evaluateRapidsExpression("(tmp= bool_frame (> (cols dataset 3) 5))");
        assertEquals("bool_frame", frameResult.key.name);
        assertEquals(1, frameResult.numCols);
        assertEquals(10000, frameResult.numRows);

        RapidsNumberV3 numberResult = (RapidsNumberV3) conn.evaluateRapidsExpression("(flatten (cols (rows dataset 0) 1))");
        assertEquals(-28.3208, numberResult.scalar, 0.0001);

        RapidsStringV3 stringResult = (RapidsStringV3) conn.evaluateRapidsExpression("(flatten (cols (rows dataset 0) 8))");
        assertEquals("c8.l6", stringResult.string);
    }

}