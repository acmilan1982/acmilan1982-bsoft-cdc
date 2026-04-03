package com.bsoft.server.sql.doris.http;

/**
 * ClassName: DorisConnectionInfo
 * Package:   com.bsoft.server.dbmeta.doris.http
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/10/9 - 17:28
 */
// Doris连接信息
public  class DorisConnectionInfo {
    private  String host;
    private  int httpPort;
    private  String database;

    public DorisConnectionInfo(String host, int httpPort, String database) {
        this.host = host;
        this.httpPort = httpPort;
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}