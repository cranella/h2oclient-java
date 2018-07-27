package com.att.h2o.proxies.retrofit;

import com.att.h2o.FileUpload;
import okhttp3.MultipartBody;
import retrofit2.Call;
import retrofit2.http.Multipart;
import retrofit2.http.POST;
import retrofit2.http.Part;

public interface PostFile {
    @Multipart
    @POST("/3/PostFile")
    Call<FileUpload> postFile(@Part MultipartBody.Part file);
}
