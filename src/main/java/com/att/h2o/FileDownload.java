package com.att.h2o;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class FileDownload {
    public String path;
    @SerializedName("content_length")
    public long contentLength;
    @SerializedName("bytes_received")
    public long bytesReceived = 0L;
    public float progress = 0F;
    public boolean complete = false;

    public FileDownload(String path, long contentLength, long bytesReceived, boolean complete) {
        this.path = path;
        this.contentLength = contentLength;
        this.bytesReceived = bytesReceived;
        this.progress = contentLength == -1 ? -1 : (float) bytesReceived/contentLength;
        this.complete = complete;
    }

    @Override
    public String toString() {
        return (new Gson()).toJson(this);
    }
}
