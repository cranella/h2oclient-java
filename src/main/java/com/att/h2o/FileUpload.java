package com.att.h2o;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class FileUpload {

    @SerializedName("destination_frame")
    public String destinationFrame;
    @SerializedName("total_bytes")
    public int totalBytes = -1;

    public FileUpload() {
    }

    public String toString() {
        return (new Gson()).toJson(this);
    }
}
