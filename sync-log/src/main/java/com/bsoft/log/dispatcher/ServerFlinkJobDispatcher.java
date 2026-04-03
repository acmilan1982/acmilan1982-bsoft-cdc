package com.bsoft.log.dispatcher;


import com.bsoft.common.config.properties.LogServerInfo;
import com.bsoft.log.k2db.LogPersistenceJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * ClassName: FlinkJobManager
 * Package:   com.bsoft.client.manager
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/23 - 18:11
 */
@Component
public class ServerFlinkJobDispatcher implements CommandLineRunner {
    private static final Logger logger = LogManager.getLogger(ServerFlinkJobDispatcher.class);


    // 服务端信息
    @Autowired
    private LogServerInfo logServerInfo;


    @Autowired
    private LogPersistenceJob logPersistenceJob;





    // 程序启动时，执行当前方法
    public void run(String... args) throws Exception {
        try {
            logPersistenceJob.init();
            logPersistenceJob.startJob();


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("重启 CDC 任务失败", e);
        }
    }

    public synchronized void reloadJob() {
        try {
//            JobClient jobClient = oracleToKafka.getJobClient();
//            // cancel 当前 Flink job
//            if (jobClient != null) {
//                jobClient.cancel().get(); // 等待任务取消完成
//            }
//
//            // 清除当前job的内存状态
//            clearJobState();
//            // 重新运行当前job
//            run();

        } catch (Exception e) {
            throw new RuntimeException("重启 CDC 任务失败", e);
        }
    }



    public  void clearJobState() {
//        configFetcher.clearState();
//        oracleToKafka.clearState();
    }


}
