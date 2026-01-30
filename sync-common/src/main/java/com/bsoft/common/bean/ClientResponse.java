package com.bsoft.common.bean;

/**
 * ClassName: ClientResponse
 * Package:   com.bsoft.bean
 * Description: 客户端统一响应格式
 *
 * @Author: snoopy
 * @Create: 2025/7/28 - 17:43
 */
public class ClientResponse {
    private int code;
    private ResponseBody body;

    public ClientResponse() {
    }

    public ClientResponse(int code, String message) {
        this.code = code;
        this.body = new ResponseBody(message);
    }

    public static ClientResponse success(String message) {
        return new ClientResponse(200, message);
    }

    public static ClientResponse error(int code, String message) {
        return new ClientResponse(code, message);
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public ResponseBody getBody() {
        return body;
    }

    public void setBody(ResponseBody body) {
        this.body = body;
    }

    /**
     * 内部响应体类
     */
    public static class ResponseBody {
        private String message;

        public ResponseBody() {
        }

        public ResponseBody(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}