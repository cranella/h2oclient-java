package com.att.h2o.examples;

import com.att.h2o.*;
import com.att.h2o.rapids.ArrayIndices;
import com.att.h2o.rapids.MergeOption;
import okhttp3.HttpUrl;

import java.io.File;

public class Rapids_Example {

    public static void main(String[] args) {

        HttpUrl url = new HttpUrl.Builder().scheme("http").host("localhost").port(54321).build();
        try (H2OConnection conn = H2OConnection.newInstance(url)) {

            H2OFrame dataset = conn.uploadFileToFrame(new File("src/test/resources/data/dataset1.csv"), "dataset1");

            H2OFrame[] splits = dataset.split(new float[] {0.75F}, new String[] {"frame1","frame2"}, 906317);
            H2OFrame frame1 = splits[0];
            H2OFrame frame2 = splits[1];

            FrameV4 summary1 = frame1.getSummary();
            System.out.println("Frame1: " + summary1);

            FrameV4 summary2 = frame2.getSummary();
            System.out.println("Frame2: " + summary2);

            H2OFrame combinedFrame = frame1.rowbind(frame2).assign("combined");
            System.out.println("Combined: " + combinedFrame.getSummary());

            H2OFrame mergedFrame = frame1.merge(frame2, MergeOption.INNER, new String[] {"C8","C10"},new String[] {"C8","C10"}).assign("merged_result");
            System.out.println("Merged: " + mergedFrame.getSummary());

            H2OFrame selected = frame1.select(frame1.select("C5").compare(">",5)
                    .and(frame1.select("C5").compare("<",10))).assign("selected");
            System.out.println("Selected: " + selected.getSummary());

            frame1.update(frame1.select("C5").compare(">",5), new ArrayIndices(0), 888).assign("updated");

            H2OFrame sum = frame1.select("C2").add(frame1.select("C3")).setColNames(new int[] {0},new String[] {"Sum"}).assign("summed");
            System.out.println("Summed: " + sum.getSummary());

            System.out.println(frame1.select(new ArrayIndices(0), new ArrayIndices(4)).flatten());

            H2OFrame grouped = frame1.groupBy(new String[] {"C8"}).count("all").getFrame("grouped");
            System.out.println(grouped);

//            FileDownloadListener listener = frame1.download(new File("C:\\Users\\username\\download_example.csv"));
//            while(listener.isDownloading()) {
//                System.out.println(listener.getStatus());
//                try {
//                    Thread.sleep(200);
//                } catch (InterruptedException e) {
//
//                }
//            }
//            if(!listener.isSuccess()) {
//                System.out.println(listener.getErrorMsg());
//            } else {
//                System.out.println(listener.getStatus());
//            }

        } catch (H2OException e) {
            e.printStackTrace();
            System.err.println(e.getH2oExceptionType());
            System.err.println("\nH2O Stacktrace:");
            for(String s : e.getH2oStacktrace()) {
                System.err.println(s);
            }
        }
    }
}
