package com.att.h2o.proxies.retrofit;

import com.att.h2o.FrameV4;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface FrameSummary {

    @GET("/3/Frames/{frame_id}/summary")
    Call<FrameV4> summary(@Path("frame_id") String var1);

}
