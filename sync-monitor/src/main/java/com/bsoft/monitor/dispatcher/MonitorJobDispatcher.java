package com.bsoft.monitor.dispatcher;

import com.bsoft.monitor.k2zk.MonitorMetricJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class MonitorJobDispatcher implements CommandLineRunner {

    @Autowired
    private MonitorMetricJob monitorMetricJob;

    @Override
    public void run(String... args) throws Exception {
        monitorMetricJob.init();
        monitorMetricJob.startJob();
    }
}
