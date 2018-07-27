package com.att.h2o;

public class FileDownloadListener {

    private volatile FileDownload status;
    private volatile boolean downloading = true;
    private volatile String errorMsg;
    private volatile boolean success;

    FileDownloadListener() {
    }

    public FileDownload getStatus() {
        return status;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public boolean isDownloading() {
        return downloading;
    }

    public boolean isSuccess() {
        return success;
    }

    synchronized void setStatus(FileDownload fileDownload) {
        this.status = fileDownload;
        this.downloading = !fileDownload.complete;
        if(fileDownload.complete) {
            this.success = true;
        }
    }

    synchronized void setError(String errorMsg) {
        this.errorMsg = errorMsg;
        this.downloading = false;
        this.success = false;
    }

}
