package com.att.h2o;

import water.bindings.pojos.H2OErrorV3;

import java.util.Map;

public class H2OException extends Exception {

    private long timestamp = 0L;
    private String errorUrl = null;
    private String h2oMsg = null;
    private String devMsg = null;
    private int httpStatus = 0;
    private Map<String, Object> values;
    private String h2oExceptionType = null;
    private String h2oExceptionMsg = null;
    private String[] h2oStacktrace;

    public H2OException(String message) {
        super(message);
    }

    public H2OException(String message, Throwable cause) {
        super(message, cause);
    }

    public H2OException(H2OErrorV3 error) {
        super(error.msg);
        this.timestamp = error.timestamp;
        this.errorUrl = error.errorUrl;
        this.h2oMsg = error.msg;
        this.devMsg = error.devMsg;
        this.httpStatus = error.httpStatus;
        this.values = error.values;
        this.h2oExceptionType = error.exceptionType;
        this.h2oExceptionMsg = error.exceptionMsg;
        this.h2oStacktrace = error.stacktrace;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getErrorUrl() {
        return errorUrl;
    }

    public String getH2oMsg() {
        return h2oMsg;
    }

    public String getDevMsg() {
        return devMsg;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public String getH2oExceptionType() {
        return h2oExceptionType;
    }

    public String getH2oExceptionMsg() {
        return h2oExceptionMsg;
    }

    public String[] getH2oStacktrace() {
        return h2oStacktrace;
    }


}
