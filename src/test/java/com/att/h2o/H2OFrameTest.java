package com.att.h2o;

import com.att.h2o.rapids.ArrayIndices;
import com.att.h2o.rapids.MergeOption;
import okhttp3.HttpUrl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import static com.att.h2o.rapids.ArrayIndices.ALL;
import static org.junit.Assert.*;

public class H2OFrameTest {

    private static H2OConnection conn;
    private static H2OFrame frame1;
    private static H2OFrame frame2;
    private static H2OFrame frame3;

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

        frame1 = conn.uploadFileToFrame(new File("src/test/resources/data/arrhythmia.csv.gz"), "arrhythmia");
        frame2 = conn.uploadFileToFrame(new File("src/test/resources/data/dataset2.csv"), "dataset2");
        frame3 = conn.uploadFileToFrame(new File("src/test/resources/data/dataset3.csv"), "dataset3");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        conn.removeAll();
        conn.close();
    }

    @Test
    public void assign() throws Exception {
        H2OFrame newFrame1 = frame1.createCopy("frame1Copy");
        assertNotNull(conn.getFrame("frame1Copy"));
        newFrame1.assign("newName");
        assertEquals("newName", newFrame1.getFrameId());
        H2OFrame retrievedFrame = conn.getFrame("newName");
        assertNotNull(retrievedFrame);
        assertEquals("newName", retrievedFrame.getFrameId());
        assertEquals(280, retrievedFrame.getSummary().numColumns);
        assertEquals(452, retrievedFrame.getSummary().rows);
        assertEquals(75.0, retrievedFrame.select(new ArrayIndices(0), new ArrayIndices(0)).flatten());
        conn.remove("newName");
        H2OFrame oldFrame = conn.getFrame(frame1.getFrameId());
        assertNotNull(oldFrame);
        assertEquals("arrhythmia", oldFrame.getFrameId());

    }

    @Test
    public void execute() throws Exception {
        H2OFrame newFrame1 = frame1.select("C1").execute();
        assertNotNull(newFrame1.getFrameId());
        assertEquals(newFrame1.getFrameId(), newFrame1.getRapidsBuilder().getAst().toString());
        assertEquals(1, newFrame1.getSummary().numColumns);
        assertEquals(452, newFrame1.getSummary().rows);
        assertEquals(75.0, newFrame1.select(0,0).flatten());

        H2OFrame newFrame2 = frame2.update(ALL,new ArrayIndices(5,7), frame3.select(ALL, new ArrayIndices(8,10))).execute();
        assertNotNull(newFrame2.getFrameId());
        assertEquals(newFrame2.getFrameId(), newFrame2.getRapidsBuilder().getAst().toString());
        assertEquals(11, newFrame2.getSummary().numColumns);
        assertEquals(10000, newFrame2.getSummary().rows);
        assertEquals(89.907, (double) newFrame2.select(0,5).flatten(), 0.001);
        assertEquals(-43.317, (double) newFrame2.select(0,6).flatten(), 0.001);

        H2OFrame newFrame3 = frame2.update(new ArrayIndices(5,10), ALL, frame2.select(new ArrayIndices(0,5), ALL));
        assertEquals(11, newFrame3.getSummary().numColumns);
        assertEquals(10000, newFrame3.getSummary().rows);
        assertEquals(-76.179, (double) newFrame3.select(5,5).flatten(), 0.001);
        assertEquals("c9.l88", newFrame3.select(6,9).flatten());

        H2OFrame newFrame4 = frame2.update("C3", frame3.select("C6"));
        assertEquals(11, newFrame4.getSummary().numColumns);
        assertEquals(10000, newFrame4.getSummary().rows);
        assertEquals(16.0, newFrame4.select(0,3).flatten());

        H2OFrame newFrame5 = frame2.update(new String[] {"C2", "C4", "C6"}, frame3.select(new String[] {"C3", "C5", "C8"}));
        assertEquals(11, newFrame5.getSummary().numColumns);
        assertEquals(10000, newFrame5.getSummary().rows);
        assertEquals("c3.l27", newFrame5.select(0,2).flatten());
        assertEquals(70.060, (double) newFrame5.select(0,4).flatten(), 0.001);
        assertEquals(89.907, (double) newFrame5.select(0,6).flatten(), 0.001);

        // H2OError "unimplemented" currently being thrown by server for frame insertion with boolean selection:

//        H2OFrame newFrame6 = frame2.update(frame2.select("response").compare("==", 1), new ArrayIndices(1,3),
//                frame3.select(frame2.select("response").compare("==", 1)).select(ALL, new ArrayIndices(new long[] {6,10})));
//        assertEquals(11, newFrame6.getSummary().colCount);
//        assertEquals(10000, newFrame6.getSummary().rowCount);
//        assertEquals("c10.l79", newFrame6.select(0,2));

        //compare
        H2OFrame newFrame7 = frame2.select("C3").compare(">", 5.0).execute();
        assertEquals(1.0, newFrame7.select(2,0).flatten());

        H2OFrame newFrame8 = frame2.select("C3").compare("<", frame3.select("C1")).execute();
        assertEquals(1.0, newFrame8.select(0,0).flatten());

        //add
        H2OFrame newFrame9 = frame2.select("C3").add(5.0).execute();
        assertEquals(-86.0, newFrame9.select(0,0).flatten());

        H2OFrame newFrame10 = frame2.select("C3").add(frame2.select("C10")).execute();
        assertEquals(-50.0, newFrame10.select(0,0).flatten());

        //subtract
        H2OFrame newFrame11 = frame2.select("C3").subtract(5.0).execute();
        assertEquals(-96.0, newFrame11.select(0,0).flatten());

        H2OFrame newFrame12 = frame2.select("C3").subtract(frame2.select("C10")).execute();
        assertEquals(-132.0, newFrame12.select(0,0).flatten());

        //multiply
        H2OFrame newFrame13 = frame2.select("C3").multiplyBy(5.0).execute();
        assertEquals(100.0, newFrame13.select(3,0).flatten());

        H2OFrame newFrame14 = frame2.select("C3").multiplyBy(frame2.select("C10")).execute();
        assertEquals(-70.0, newFrame14.select(5,0).flatten());

        //divide
        H2OFrame newFrame15 = frame2.select("C3").divideBy(5.0).execute();
        assertEquals(-18.2, newFrame15.select(0,0).flatten());

        H2OFrame newFrame16 = frame2.select("C3").divideBy(frame2.select("C10")).execute();
        assertEquals(-17.5, newFrame16.select(5,0).flatten());

        //int divide
        H2OFrame newFrame17 = frame2.select("C3").floorDivide(5.0).execute();
        assertEquals(-18.0, newFrame17.select(0,0).flatten());

        H2OFrame newFrame18 = frame2.select("C3").floorDivide(frame2.select("C10")).execute();
        assertEquals(-17.0, newFrame18.select(5,0).flatten());

        //mod
        H2OFrame newFrame19 = frame2.select("C3").mod(5.0).execute();
        assertEquals(3.0, newFrame19.select(6,0).flatten());

        H2OFrame newFrame20 = frame2.select("C3").mod(frame2.select("C10")).execute();
        assertEquals(-1.0, newFrame20.select(5,0).flatten());

        //power
        H2OFrame newFrame21 = frame2.select("C3").pow(2.0).execute();
        assertEquals(9.0, newFrame21.select(8,0).flatten());

        H2OFrame newFrame22 = frame2.select("C3").pow(frame2.select("response")).execute();
        assertEquals(-35.0, newFrame22.select(5,0).flatten());

        //convert to factor
        H2OFrame newFrame23 = frame2.select("response").asFactor().execute();
        assertEquals("enum", newFrame23.getColSummary("response").type);

        //convert to numeric
        H2OFrame newFrame24 = newFrame23.asNumeric().execute();
        assertEquals("int", newFrame24.getColSummary("response").type);

        //rowbind
        H2OFrame newFrame25 = frame2.rowbind(frame2).execute();
        assertEquals(20000, newFrame25.getSummary().rows);
        assertEquals(11, newFrame25.getSummary().numColumns);

        //colbind
        H2OFrame newFrame26 = frame2.colbind(frame3).execute();
        assertEquals(10000, newFrame26.getSummary().rows);
        assertEquals(22, newFrame26.getSummary().numColumns);

        //set colnames
        H2OFrame newFrame27 = frame1.setColNames(new int[] {0,1,2}, new String[] {"firstCol", "secondCol", "thirdCol"}).execute();
        assertEquals("firstCol", newFrame27.getSummary().columns[0].label);
        assertEquals("secondCol", newFrame27.getSummary().columns[1].label);
        assertEquals("thirdCol", newFrame27.getSummary().columns[2].label);

        //merge
        H2OFrame left = frame2.select(new ArrayIndices(0,100), new ArrayIndices(0,3)).execute();
        H2OFrame right = frame2.select(new ArrayIndices(100,200), new ArrayIndices(2,4)).execute();
        H2OFrame newFrame28 = left.merge(right, MergeOption.INNER, new String[] {"C2"}, new String[] {"C2"}).execute();
        assertEquals(4, newFrame28.getSummary().numColumns);
        assertEquals(80, newFrame28.getSummary().rows);
        assertEquals(29.0, newFrame28.select(0,3).flatten());
    }

    @Test
    public void createCopy() throws Exception {
        H2OFrame newFrame1 = frame1.createCopy("newFrame1");
        FrameV4 frame1SummaryInitial = frame1.getSummary();
        FrameV4 newFrame1SummaryInitial = newFrame1.getSummary();
        assertNotNull(newFrame1);
        assertNotNull(conn.getFrame("newFrame1"));
        newFrame1.createCopy("newFrame2");
        H2OFrame newFrame2 = conn.getFrame("newFrame2");
        FrameV4 newFrame2SummaryInitial = newFrame2.getSummary();
        assertEquals(Arrays.toString(newFrame1SummaryInitial.columns), Arrays.toString(newFrame2SummaryInitial.columns));
        newFrame1.update(new ArrayIndices(0), new ArrayIndices(0), 5).assign("newFrame1");
        assertEquals(5.0, newFrame1.select(new ArrayIndices(0), new ArrayIndices(0)).flatten());
        assertEquals(75.0, newFrame2.select(new ArrayIndices(0), new ArrayIndices(0)).flatten());
        newFrame2 = conn.getFrame("newFrame2");
        assertEquals(newFrame2SummaryInitial.toString(), newFrame2.getSummary().toString());
        conn.remove("newFrame1");
        conn.remove("newFrame2");
        assertEquals(frame1SummaryInitial.toString(), conn.getFrame(frame1.getFrameId()).toString());
        assertEquals(75.0, frame1.select(new ArrayIndices(0), new ArrayIndices(0)).flatten());
    }

    @Test
    public void select_SingleIndex()  throws Exception {
        H2OFrame newFrame = frame1.select(5,5);
        assertEquals("(rows (cols arrhythmia 5) 5)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void select_ArrayIndices() throws Exception {
        H2OFrame newFrame1 = frame1.select(new ArrayIndices(0), new ArrayIndices(0));
        assertEquals("(rows (cols arrhythmia 0) 0)", newFrame1.getRapidsBuilder().getAst().toString());

        H2OFrame newFrame2 = frame1.select(new ArrayIndices(5,10),new ArrayIndices(3,5));
        assertEquals("(rows (cols arrhythmia [3:2]) [5:5])", newFrame2.getRapidsBuilder().getAst().toString());

        H2OFrame newFrame3 = frame1.select(ALL,new ArrayIndices(0,5));
        assertEquals("(cols arrhythmia [0:5])", newFrame3.getRapidsBuilder().getAst().toString());

        H2OFrame newFrame4 = frame1.select(new ArrayIndices(0,5), ALL);
        assertEquals("(rows arrhythmia [0:5])", newFrame4.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void select_ColString() throws Exception {
        H2OFrame newFrame = frame1.select("C1");
        assertEquals("(cols arrhythmia 'C1')", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void select_ColStrings() throws Exception {
        H2OFrame newFrame = frame1.select(new String[] {"C1", "C2", "C3"});
        assertEquals("(cols arrhythmia ['C1', 'C2', 'C3'])", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void select_BoolFrame() throws Exception {
        H2OFrame newFrame = frame1.select(frame1.select("C1").compare(">",5));
        assertEquals("(rows arrhythmia (> (cols arrhythmia 'C1') 5.0))", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void update_ArrayIndices() throws Exception {
        H2OFrame newFrame1 = frame1.update(new ArrayIndices(0), new ArrayIndices(0), 5);
        assertEquals("(:= arrhythmia 5 0 0)", newFrame1.getRapidsBuilder().getAst().toString());
        H2OFrame newFrame2 = frame1.update(new ArrayIndices(0), new ArrayIndices(0), 5.0D);
        assertEquals("(:= arrhythmia 5.0 0 0)", newFrame2.getRapidsBuilder().getAst().toString());
        H2OFrame newFrame3 = frame2.update(new ArrayIndices(0), new ArrayIndices(2), "string");
        assertEquals("(:= dataset2 'string' 2 0)", newFrame3.getRapidsBuilder().getAst().toString());

        H2OFrame newFrame4 = frame1.update(new ArrayIndices(0,10),new ArrayIndices(0,5), 5);
        assertEquals("(:= arrhythmia 5 [0:5] [0:10])", newFrame4.getRapidsBuilder().getAst().toString());

        H2OFrame newFrame5 = frame2.update(ALL,new ArrayIndices(5,7), frame3.select(ALL, new ArrayIndices(8,10)));
        assertEquals("(:= dataset2 (cols dataset3 [8:2]) [5:2] [0:10000])", newFrame5.getRapidsBuilder().getAst().toString());

        H2OFrame newFrame6 = frame2.update(new ArrayIndices(5,10), ALL, frame2.select(new ArrayIndices(0,5), ALL));
        assertEquals("(:= dataset2 (rows dataset2 [0:5]) [0:11] [5:5])", newFrame6.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void update_ColString() throws Exception {
        H2OFrame newFrame = frame2.update("C3", frame3.select("C6"));
        assertEquals("(:= dataset2 (cols dataset3 'C6') 'C3' [])", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void update_ColStrings() throws Exception {
        H2OFrame newFrame = frame2.update(new String[] {"C2", "C4", "C6"}, frame3.select(new String[] {"C3", "C5", "C8"}));
        assertEquals("(:= dataset2 (cols dataset3 ['C3', 'C5', 'C8']) ['C2', 'C4', 'C6'] [])", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void update_BoolFrameArrayIndices() throws Exception {
        H2OFrame newFrame = frame2.update(frame2.select("response").compare("==", 1), new ArrayIndices(1,3),
                frame3.select(frame2.select("response").compare("==", 1)).select(ALL, new ArrayIndices(new long[] {6,10})));
        assertEquals("(:= dataset2 (cols (rows dataset3 (== (cols dataset2 'response') 1.0)) [6, 10]) [1:2] (== (cols dataset2 'response') 1.0))",
                newFrame.getRapidsBuilder().getAst().toString());
        //NOTE: H2OError "unimplemented" currently being thrown by server for frame insertion with boolean selection
        //Check water.rapids.ast.prims.assign.AstRectangleAssign.apply(AstRectangleAssign.java:93) for updates
    }

    @Test
    public void update_BoolFrameColStrings() throws Exception {
        H2OFrame newFrame = frame2.update(frame2.select("response").compare("==", 1), new String[] {"C1", "C2"},
                frame3.select(frame2.select("response").compare("==", 1)).select(ALL, new ArrayIndices(new long[] {6,10})));
        assertEquals("(:= dataset2 (cols (rows dataset3 (== (cols dataset2 'response') 1.0)) [6, 10]) ['C1', 'C2'] (== (cols dataset2 'response') 1.0))",
                newFrame.getRapidsBuilder().getAst().toString());
        //NOTE: H2OError "unimplemented" currently being thrown by server for frame insertion with boolean selection
        //Check water.rapids.ast.prims.assign.AstRectangleAssign.apply(AstRectangleAssign.java:93) for updates
    }

    @Test
    public void compare_Double() throws Exception {
        H2OBoolFrame newFrame = frame1.compare("==", 5.0);
        assertEquals("(== arrhythmia 5.0)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void compare_Frame() throws Exception {
        H2OBoolFrame newFrame = frame2.compare("==", frame3);
        assertEquals("(== dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void add_Double() throws Exception {
        H2OFrame newFrame = frame1.add(5);
        assertEquals("(+ arrhythmia 5.0)",newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void add_Frame() throws Exception {
        H2OFrame newFrame = frame2.add(frame3);
        assertEquals("(+ dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void subtract_Double() throws Exception {
        H2OFrame newFrame = frame1.subtract(5);
        assertEquals("(- arrhythmia 5.0)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void subtract_Frame() throws Exception {
        H2OFrame newFrame = frame2.subtract(frame3);
        assertEquals("(- dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void multiplyBy_Double() throws Exception {
        H2OFrame newFrame = frame1.multiplyBy(5);
        assertEquals("(* arrhythmia 5.0)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void multiplyBy_Frame() throws Exception {
        H2OFrame newFrame = frame2.multiplyBy(frame3);
        assertEquals("(* dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void divideBy_Double() throws Exception {
        H2OFrame newFrame = frame1.divideBy(5);
        assertEquals("(/ arrhythmia 5.0)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void divideBy_Frame() throws Exception {
        H2OFrame newFrame = frame2.divideBy(frame3);
        assertEquals("(/ dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void floorDivide_Double() throws Exception {
        H2OFrame newFrame = frame1.floorDivide(5);
        assertEquals("(intDiv arrhythmia 5.0)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void floorDivide_Frame() throws Exception {
        H2OFrame newFrame = frame2.floorDivide(frame3);
        assertEquals("(intDiv dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void mod_Double() throws Exception {
        H2OFrame newFrame = frame1.mod(5);
        assertEquals("(% arrhythmia 5.0)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void mod_Frame() throws Exception {
        H2OFrame newFrame = frame2.mod(frame3);
        assertEquals("(% dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void pow_Double() throws Exception {
        H2OFrame newFrame = frame1.pow(5);
        assertEquals("(^ arrhythmia 5.0)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void pow_Frame() throws Exception {
        H2OFrame newFrame = frame2.pow(frame3);
        assertEquals("(^ dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void asFactor() throws Exception {
        H2OFrame newFrame = frame2.select("response").asFactor();
        assertEquals("(as.factor (cols dataset2 'response'))", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void asNumeric() throws Exception {
        H2OFrame newFrame = frame2.select("response").asNumeric();
        assertEquals("(as.numeric (cols dataset2 'response'))", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void split() throws Exception {
        H2OFrame[] newFrames = frame2.split(new float[] {0.5F, 0.25F}, new String[] {"split1", "split2", "split3"}, 99);
        assertNotNull(conn.getFrame("split1"));
        assertNotNull(conn.getFrame("split2"));
        assertNotNull(conn.getFrame("split3"));
        H2OFrame split1 = newFrames[0];
        H2OFrame split2 = newFrames[1];
        H2OFrame split3 = newFrames[2];
        assertEquals(frame2.getSummary().numColumns, split1.getSummary().numColumns);
        assertEquals(frame2.getSummary().numColumns, split2.getSummary().numColumns);
        assertEquals(frame2.getSummary().numColumns, split3.getSummary().numColumns);
        long originalRows = frame2.getSummary().rows;
        assertTrue(split1.getSummary().rows <= 0.52*originalRows && split1.getSummary().rows >= 0.48*originalRows);
        assertTrue(split2.getSummary().rows <= 0.27*originalRows && split1.getSummary().rows >= 0.23*originalRows);
        assertTrue(split3.getSummary().rows <= 0.27*originalRows && split1.getSummary().rows >= 0.23*originalRows);
    }

    @Test
    public void flatten() throws Exception {
        double scalar = (Double) frame1.select(0,0).flatten();
        assertEquals(75.0, scalar, 0);
        String string = (String) frame2.select(0,2).flatten();
        assertEquals("c2.l24", string);
    }

    @Test
    public void groupBy_ColumnNames() throws Exception {
        H2OGroupBy groupBy1 = frame2.groupBy(new String[] {"C2"});
        assertNotNull(groupBy1);
        H2OFrame counts1 = groupBy1.count("all").getFrame();
        assertEquals(101, counts1.getSummary().rows);
        assertEquals(2, counts1.getSummary().numColumns);
        assertEquals(106.0, counts1.select(0,1).flatten());
        assertEquals(94.0, counts1.select(1,1).flatten());

        H2OGroupBy groupBy2 = frame2.groupBy(new String[] {"C2", "C9"});
        assertNotNull(groupBy2);
        H2OFrame counts2 = groupBy2.count("all").getFrame();
        assertEquals(6380, counts2.getSummary().rows);
        assertEquals(3, counts2.getSummary().numColumns);
        assertEquals(5.0, counts2.select(97,2).flatten());
    }

    @Test
    public void groupBy_ColumnIndices() throws Exception {
        H2OGroupBy groupBy1 = frame2.groupBy(new int[] {2});
        assertNotNull(groupBy1);
        H2OFrame counts1 = groupBy1.count("all").getFrame();
        assertEquals(101, counts1.getSummary().rows);
        assertEquals(2, counts1.getSummary().numColumns);
        assertEquals(106.0, counts1.select(0,1).flatten());
        assertEquals(94.0, counts1.select(1,1).flatten());

        H2OGroupBy groupBy2 = frame2.groupBy(new int[] {2, 9});
        assertNotNull(groupBy2);
        H2OFrame counts2 = groupBy2.count("all").getFrame();
        assertEquals(6380, counts2.getSummary().rows);
        assertEquals(3, counts2.getSummary().numColumns);
        assertEquals(5.0, counts2.select(97,2).flatten());
    }

    @Test
    public void rowbind() throws Exception {
        H2OFrame newFrame = frame2.rowbind(frame3);
        assertEquals("(rbind dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void colbind() throws Exception {
        H2OFrame newFrame = frame2.colbind(frame3);
        assertEquals("(cbind dataset2 dataset3)", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void merge_ColIndices() throws Exception {
        H2OFrame newFrame1 = frame2.merge(frame3, MergeOption.LEFT, new int[] {1,2,3}, new int[] {3,4,5});
        assertEquals("(merge dataset2 dataset3 true false [1, 2, 3] [3, 4, 5] 'auto')", newFrame1.getRapidsBuilder().getAst().toString());
        H2OFrame newFrame2 = frame2.merge(frame3, MergeOption.RIGHT, new int[] {1,2,3}, new int[] {3,4,5});
        assertEquals("(merge dataset2 dataset3 false true [1, 2, 3] [3, 4, 5] 'auto')", newFrame2.getRapidsBuilder().getAst().toString());
        H2OFrame newFrame3 = frame2.merge(frame3, MergeOption.INNER, new int[] {1,2,3}, new int[] {3,4,5});
        assertEquals("(merge dataset2 dataset3 false false [1, 2, 3] [3, 4, 5] 'auto')", newFrame3.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void merge_ColNames() throws Exception {
        H2OFrame newFrame = frame2.merge(frame3, MergeOption.LEFT, new String[] {"C1","C2","C3"}, new String[] {"C3","C4","C5"});
        assertEquals("(merge dataset2 dataset3 true false [1, 2, 3] [3, 4, 5] 'auto')", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void setColNames_Indices() throws Exception {
        H2OFrame newFrame = frame1.setColNames(new int[] {0,1,2}, new String[] {"firstCol", "secondCol", "thirdCol"});
        assertEquals("(colnames= arrhythmia [0 1 2 ] ['firstCol' 'secondCol' 'thirdCol' ])", newFrame.getRapidsBuilder().getAst().toString());
    }

    @Test
    public void setColNames() throws Exception {
        H2OFrame newFrame = frame1.setColNames(new String[] {"C1","C2","C3"}, new String[] {"firstCol", "secondCol", "thirdCol"});
        assertEquals("(colnames= arrhythmia [0 1 2 ] ['firstCol' 'secondCol' 'thirdCol' ])", newFrame.getRapidsBuilder().getAst().toString());
    }

}