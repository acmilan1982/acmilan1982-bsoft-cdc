package com.bsoft.common.bean;

import com.bsoft.common.dataSource.DataSourceFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.io.Serializable;
import java.util.UUID;

/**
 *
 * 配置库的连接信息
 * ClassName: ConfigDbProperties
 * Package:   com.bsoft.config
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/3 - 19:24
 */
@Component
@ConfigurationProperties(prefix = "config-db")
public class ConfigDataSourceConfig  implements Serializable {

    // 添加实例唯一标识
    private final String instanceId = UUID.randomUUID().toString().substring(0, 8);

    private String url;
    private String username;
    private String password;
    private String driverClassName;   // 属性名可用驼峰，自动映射 driver-class-name

    // 添加连接池配置属性（带默认值）
    private int maximumPoolSize = 20;
    private int minimumIdle = 16;
    private long connectionTimeout = 30000;
    private long idleTimeout = 600000;



    public ConfigDataSourceConfig() {
    }

    @PostConstruct
    public DataSource initDataSource() {
        return DataSourceFactory.getDataSource(this);
    }

    @PreDestroy
    public void closeDataSource() {
//        if (dataSource instanceof HikariDataSource) {
//            ((HikariDataSource) dataSource).close();
//        }
    }


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }


    public String getInstanceId() {
        return instanceId;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    public int getMinimumIdle() {
        return minimumIdle;
    }

    public void setMinimumIdle(int minimumIdle) {
        this.minimumIdle = minimumIdle;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }


}

