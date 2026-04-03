package com.bsoft.server.configuration;

import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.util.ZooKeeperNodeManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * ClassName: ZooKeeperConfiguration
 * Package:   com.bsoft.configuration
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/30 - 10:42
 */
// 在配置类中注册为Bean
@Configuration
public class ZooKeeperConfiguration {

    @Bean
    public ZooKeeperNodeManager zooKeeperNodeManager(ServerInfo serverInfo) throws Exception {
        return new ZooKeeperNodeManager(serverInfo);
    }
}
