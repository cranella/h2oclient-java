package com.att.h2o.proxies.retrofit;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;
import retrofit2.http.Streaming;

public interface DownloadDataset {
    @Streaming
    @GET("/3/DownloadDataset")
    Call<ResponseBody> fetch(@Query("frame_id") String var1);
}

