package com.bsoft.common.bean;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * ClassName: StateBackendProperties
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/10 - 14:24
 */
@Component
@ConfigurationProperties(prefix = "state-backend")
public class StateBackendProperties {
    private String checkpointPath;
    private boolean enableIncremental;

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public void setCheckpointPath(String checkpointPath) {
        this.checkpointPath = checkpointPath;
    }

    public boolean isEnableIncremental() {
        return enableIncremental;
    }

    public void setEnableIncremental(boolean enableIncremental) {
        this.enableIncremental = enableIncremental;
    }
}
