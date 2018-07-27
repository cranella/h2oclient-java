package com.att.h2o.proxies.retrofit;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.Field;
import retrofit2.http.GET;
import retrofit2.http.Path;
import retrofit2.http.Streaming;

public interface DownloadModels {

    @Streaming
    @GET("/3/Models.java/{model_id}")
    Call<ResponseBody> fetchJavaCode(@Path("model_id") String var1, @Field("preview") boolean var2, @Field("find_compatible_frames") boolean var3, @Field("_exclude_fields") String var4);

    @Streaming
    @GET("/3/Models.java/{model_id}")
    Call<ResponseBody> fetchJavaCode(@Path("model_id") String var1);

    @Streaming
    @GET("/3/Models/{model_id}/mojo")
    Call<ResponseBody> fetchMojo(@Path("model_id") String var1, @Field("preview") boolean var2, @Field("find_compatible_frames") boolean var3, @Field("_exclude_fields") String var4);

    @Streaming
    @GET("/3/Models/{model_id}/mojo")
    Call<ResponseBody> fetchMojo(@Path("model_id") String var1);

}
