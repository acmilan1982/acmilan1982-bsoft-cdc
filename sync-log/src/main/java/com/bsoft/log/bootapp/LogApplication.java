package com.bsoft.log.bootapp;



import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot 启动类
 * 独立放置在 com.bsoft.bootapp 包下
 */
@SpringBootApplication(scanBasePackages = "com.bsoft") // 扫描 com.bsoft 下的所有组件
public class LogApplication {

    public static void main(String[] args) {
        // 启动 Spring Boot 应用
        SpringApplication.run(LogApplication.class, args);
    }
}