package com.att.h2o;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ProgressRequestBody extends RequestBody {
    private File file;
    private static final int DEFAULT_BUFFER_SIZE = 2048;

    ProgressRequestBody(File file) {
        this.file = file;
    }

    @Override
    public MediaType contentType() {
        return MediaType.parse("text/plain");
    }

    @Override
    public void writeTo(BufferedSink bufferedSink) throws IOException {
        long fileLength = file.length();
        long startTime = System.currentTimeMillis();
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        AtomicLong bytesUploaded = new AtomicLong(0);

        try(FileInputStream input = new FileInputStream(file)) {
            int bytesRead;
            Thread consoleOutput = new Thread(new ProgressUpdater(bytesUploaded, fileLength, startTime));
            System.out.println("File " + file.getPath() + " upload progress:");
            consoleOutput.start();
            while ((bytesRead = input.read(buffer)) != -1) {
                bytesUploaded.getAndAdd(bytesRead);
                bufferedSink.write(buffer, 0, bytesRead);
            }
        }
    }

    private class ProgressUpdater implements Runnable {
        private AtomicLong current;
        private long total;
        private long startTime;

        public ProgressUpdater(AtomicLong current, long total, long startTime) {
            this.current = current;
            this.total = total;
            this.startTime = startTime;
        }

        @Override
        public void run() {
            long bytes = current.get();

            while(bytes <= total) {
                long eta = current.get() == 0 ? 0 :
                        (total - bytes) * (System.currentTimeMillis() - startTime) / bytes;

                StringBuilder string = new StringBuilder(140);
                int percent = (int) (bytes * 100 / total);
                string
                        .append('\r')
                        .append(String.join("", Collections.nCopies(percent == 0 ? 2 : 2 - (int) (Math.log10(percent)), " ")))
                        .append(String.format(" %d%% [", percent))
                        .append(String.join("", Collections.nCopies(percent, ".")))
                        .append(String.join("", Collections.nCopies(100 - percent, " ")))
                        .append(']')
                        .append(String.join("", Collections.nCopies(bytes == 0 ? (int) (Math.log10(total)) : (int) (Math.log10(total)) - (int) (Math.log10(bytes)), " ")));
                 if(bytes < total) {
                     String etaHms = bytes == 0 ? "N/A" :
                             String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(eta),
                                     TimeUnit.MILLISECONDS.toMinutes(eta) % TimeUnit.HOURS.toMinutes(1),
                                     TimeUnit.MILLISECONDS.toSeconds(eta) % TimeUnit.MINUTES.toSeconds(1));
                        string.append(String.format(" %d/%d, ETA: %s", bytes, total, etaHms));
                 }

                System.out.print(string);

                try {
                    Thread.sleep(240);
                } catch (InterruptedException e) {
                    //none expected
                }
                bytes = current.get();
            }
        }
    }
}
