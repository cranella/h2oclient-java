package com.att.h2o;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import water.bindings.pojos.FrameBaseV3;
import water.bindings.pojos.TwoDimTableV3;

public class FrameV4 extends FrameBaseV3 {

    @SerializedName("row_offset")
    public long rowOffset = 0L;
    @SerializedName("row_count")
    public int rowCount = 0;
    @SerializedName("column_offset")
    public int columnOffset = 0;
    @SerializedName("column_count")
    public int columnCount = 0;
    @SerializedName("full_column_count")
    public int fullColumnCount = 0;
    @SerializedName("total_column_count")
    public int totalColumnCount = 0;
    public long checksum = 0L;
    public long rows = 0L;
    @SerializedName("num_columns")
    public long numColumns = 0L;
    @SerializedName("default_percentiles")
    public double[] defaultPercentiles;
    public ColV4[] columns;
    @SerializedName("compatible_models")
    public String[] compatibleModels;
    @SerializedName("chunk_summary")
    public TwoDimTableV3 chunkSummary;
    @SerializedName("distribution_summary")
    public TwoDimTableV3 distributionSummary;

    public FrameV4() {
        this.byteSize = 0L;
        this.isText = false;
        this._excludeFields = "";
    }
    @Override
    public String toString() {
        return (new GsonBuilder().serializeSpecialFloatingPointValues().create()).toJson(this);
    }
}
