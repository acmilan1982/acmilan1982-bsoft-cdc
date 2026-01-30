package com.bsoft.server.sql.insert.http;

/**
 * ClassName: StreamLoadResult
 * Package:   com.bsoft.server.dbmeta.doris.http
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/10/9 - 17:29
 */
// Stream Load结果
public class StreamLoadResult {
    // 执行是否成功
    private boolean success;
    // 加载的行数，纯粹是为了日志打印
    private int numberLoadedRows;
    // 过滤的行数，纯粹是为了日志打印
    private int numberFilteredRows;
    // 原始响应JSON，会存储在日志中
    private String responseJson;

    public StreamLoadResult(boolean success, int numberLoadedRows, int numberFilteredRows, String responseJson) {
        this.success = success;
        this.numberLoadedRows = numberLoadedRows;
        this.numberFilteredRows = numberFilteredRows;
        this.responseJson = responseJson;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public int getNumberLoadedRows() {
        return numberLoadedRows;
    }

    public void setNumberLoadedRows(int numberLoadedRows) {
        this.numberLoadedRows = numberLoadedRows;
    }

    public int getNumberFilteredRows() {
        return numberFilteredRows;
    }

    public void setNumberFilteredRows(int numberFilteredRows) {
        this.numberFilteredRows = numberFilteredRows;
    }

    public String getResponseJson() {
        return responseJson;
    }

    public void setResponseJson(String responseJson) {
        this.responseJson = responseJson;
    }
}
