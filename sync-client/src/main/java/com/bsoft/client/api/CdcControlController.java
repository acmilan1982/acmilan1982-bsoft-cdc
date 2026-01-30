package com.bsoft.client.api;

/**
 * ClassName: CdcControlController
 * Package:   com.bsoft.client.http
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/23 - 17:48
 */


import com.bsoft.common.bean.ClientResponse;
import com.bsoft.client.dispatcher.ClientJobDispatcher;
import com.bsoft.common.enums.JobStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 控制 CDC 任务的接口，供 Web 管理平台调用。
 */
@RestController
@RequestMapping("/api")
public class CdcControlController {


    @Autowired
    private ClientJobDispatcher flinkJobDispatcher;


    // 重启标记位
    private volatile boolean restartFlag = false;

    /**
     * Web 管理平台调用此接口，触发 CDC 任务重启。
     * @return 处理结果
     */
    @PostMapping("/reload")
    public ResponseEntity<ClientResponse> reloadCdcJob() {
        try {

            // 检查是否正在重启
            if (restartFlag) {
                return ResponseEntity.ok(ClientResponse.error(500, "CDC 任务正在重启中，请稍后再试")
                );
            }

            JobStatus jobStatus = flinkJobDispatcher.getOracleToKafka().getJobStatus();
            // 这两种状态才允许刷新配置
            if(jobStatus == JobStatus.INCREMENTAL_RUNNING || jobStatus == JobStatus.SNAPSHOT_RUNNING){
                // 设置重启标记
                restartFlag = true;
                try {
                    flinkJobDispatcher.reloadJob();
                    return ResponseEntity.ok(ClientResponse.success("CDC 任务重启成功"));
                } finally {
                    // 无论成功失败都要清除重启标记
                    restartFlag = false;
                }
            }

            return ResponseEntity.ok(ClientResponse.error(501, "当前作业状态不允许该操作"));


        } catch (Exception e) {
            // 异常时也要清除重启标记
            restartFlag = false;
            return ResponseEntity.ok(ClientResponse.error(502, "CDC 任务重启失败: " + e.getMessage()));

        }
    }
}
