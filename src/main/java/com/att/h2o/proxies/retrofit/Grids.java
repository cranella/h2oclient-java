package com.att.h2o.proxies.retrofit;


import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;
import retrofit2.http.Query;
import water.bindings.pojos.GridSchemaV99;
import water.bindings.pojos.GridsV99;

public interface Grids {

    @GET("/99/Grids/{grid_id}")
    Call<GridSchemaV99> fetch(@Path("grid_id") String var1, @Query("sort_by") String var2, @Query("decreasing") boolean var3, @Query("model_ids") String[] var4);

    @GET("/99/Grids/{grid_id}")
    Call<GridSchemaV99> fetch(@Path("grid_id") String var1);

    @GET("/99/Grids")
    Call<GridsV99> list();
}
