package com.bsoft.monitor.bootapp;

import com.bsoft.common.config.properties.ClientInfo;

import com.bsoft.common.config.properties.ServerInfo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.bsoft") // 扫描 com.bsoft 下的所有组件
public class MonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MonitorApplication.class, args);
    }
}
