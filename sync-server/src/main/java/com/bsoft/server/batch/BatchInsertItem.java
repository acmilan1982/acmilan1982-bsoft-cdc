package com.bsoft.server.batch;

import com.bsoft.common.bean.KafkaPayload;

import java.util.Map;

/**
 * 批量插入项
 * ClassName: BatchInsertItem
 * Package:   com.bsoft.server.k2db.batch
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/10/11 - 20:25
 */
public class BatchInsertItem {
    private  KafkaPayload payload;
    private  Map<String, Object> data;

    public BatchInsertItem(KafkaPayload payload, Map<String, Object> data) {
        this.payload = payload;
        this.data = data;
    }


    public KafkaPayload getPayload() {
        return payload;
    }

    public void setPayload(KafkaPayload payload) {
        this.payload = payload;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
}
