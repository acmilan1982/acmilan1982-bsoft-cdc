package com.bsoft.common.http;

import java.io.Serializable;

/**
 * ClassName: ApiResponse
 * Package:   com.bsoft.common.http
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/17 - 9:45
 */
public class ApiResponse<T> implements Serializable {
    private boolean success;
    private String code;
    private String message;
    private String traceId;
    private T data;

    public static <T> ApiResponse<T> ok(T data, String traceId) {
        ApiResponse<T> r = new ApiResponse<>();
        r.success = true;
        r.code = ErrorCode.OK.name();
        r.message = "success";
        r.traceId = traceId;
        r.data = data;
        return r;
    }

    public static <T> ApiResponse<T> fail(ErrorCode code, String message, String traceId) {
        ApiResponse<T> r = new ApiResponse<>();
        r.success = false;
        r.code = code.name();
        r.message = message;
        r.traceId = traceId;
        r.data = null;
        return r;
    }

    public boolean isSuccess() { return success; }
    public String getCode() { return code; }
    public String getMessage() { return message; }
    public String getTraceId() { return traceId; }
    public T getData() { return data; }

    public void setSuccess(boolean success) { this.success = success; }
    public void setCode(String code) { this.code = code; }
    public void setMessage(String message) { this.message = message; }
    public void setTraceId(String traceId) { this.traceId = traceId; }
    public void setData(T data) { this.data = data; }
}
