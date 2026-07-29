package com.bsoft.client.bootapp;



import com.bsoft.common.config.properties.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * Spring Boot 启动类
 * 独立放置在 com.bsoft.bootapp 包下
 */
@SpringBootApplication
@ComponentScan(
        basePackages = "com.bsoft",          // 扫描 com.bsoft 下的所有组件
        excludeFilters = {
                @ComponentScan.Filter(
                        type = FilterType.ASSIGNABLE_TYPE,
                        classes = {
                                ServerInfo.class,
                                PipeLineInfo.class,
                                LogInfo.class,
                                MonitorServerInfo.class
                        }
                )
        }
)
public class ClientApplication {

    public static void main(String[] args) {
        // 启动 Spring Boot 应用
        SpringApplication.run(ClientApplication.class, args);
    }
}