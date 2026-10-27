package com.bsoft.common.bean;


import com.bsoft.common.util.DateFormatUtils;
import com.bsoft.common.util.IdGenerateUtil;

import java.time.LocalDateTime;

/**
 * CDC_LOG 表对应实体，推荐使用 builder() 构建
 */
public class SyncLogEntry {
    /** 主键 */
    public long cdcLogId;
    /** 指令类型：c=新增，u=更新，d=删除 */
    public String instructionType;
    /** LOGMNR挖掘到源数据的时间 */
    public String sourceTime;
    /** 源数据落盘到目标库的时间 */
    public String targetTime;
    /** 当前日志落盘的时间 */
    public String insertTime;
    /** 日志详情，包括落盘的内容，操作结果(成功或是错误详细)，该字段考虑压缩 */
    public String logDetail;
    /** 源库的数据源 id */
    public String sourceDataSourceId;
    /** 目标库的数据源 id */
    public String targetDataSourceId;
    /** 源库的用户名 */
    public String sourceSchemaName;
    /** 源库的表名 */
    public String sourceTableName;
    /** 目标库的表名 */
    public String targetTableName;
    /** 暂时不用（保留字段） */
    public String resultDetail;
    /** 0表示执行成功，1表示执行出现异常 */
    public Integer resultCode;
    /** 偏移量 */
    public Long offset;
    /** 数据进入Kafka的时间（数据进入链路） */
    public String kafkaEnqueueTime;
    /** 原始消息 */
    public String rawMessage;

    // 私有构造方法，强制用 builder 构造
    private SyncLogEntry() {}

    /**
     * 构建器入口
     */
    public static SyncLogEntryBuilder builder() {
        return new SyncLogEntryBuilder();
    }

    /**
     * Builder 内部类，setXxx链式设置，build()生成实体
     */
    public static class SyncLogEntryBuilder {
        private final SyncLogEntry entry = new SyncLogEntry();

        public SyncLogEntryBuilder setInstructionType(String val) {
            entry.instructionType = val;
            return this;
        }
        public SyncLogEntryBuilder setSourceTime(String val) {
            entry.sourceTime = val;
            return this;
        }

        public SyncLogEntryBuilder setTargetTime(String val) {
            entry.targetTime = val;
            return this;
        }
        public SyncLogEntryBuilder setInsertTime(String val) {
            entry.insertTime = val;
            return this;
        }
        public SyncLogEntryBuilder setLogDetail(String val) {
            entry.logDetail = val;
            return this;
        }
        public SyncLogEntryBuilder setSourceDataSourceId(String val) {
            entry.sourceDataSourceId = val;
            return this;
        }
        public SyncLogEntryBuilder setTargetDataSourceId(String val) {
            entry.targetDataSourceId = val;
            return this;
        }
        public SyncLogEntryBuilder setSourceTableName(String val) {
            entry.sourceTableName = val;
            return this;
        }
        public SyncLogEntryBuilder setTargetTableName(String val) {
            entry.targetTableName = val;
            return this;
        }
        public SyncLogEntryBuilder setResultDetail(String val) {
            entry.resultDetail = val;
            return this;
        }
        public SyncLogEntryBuilder setResultCode(Integer val) {
            entry.resultCode = val;
            return this;
        }
        public SyncLogEntryBuilder setOffset(Long val) {
            entry.offset = val;
            return this;
        }
        public SyncLogEntryBuilder setKafkaEnqueueTime(String val) {
            entry.kafkaEnqueueTime = val;
            return this;
        }


        public SyncLogEntryBuilder setRawMessage(String val) {
            entry.rawMessage = val;
            return this;
        }
        public SyncLogEntryBuilder setSourceSchemaName(String val) {
            entry.sourceSchemaName = val;
            return this;
        }

        /**
         * 构建SyncLogEntry对象，自动填充主键与insertTime
         */
        public SyncLogEntry build() {
            entry.cdcLogId = IdGenerateUtil.nextId();
//            if (entry.insertTime == null) {
//                entry.insertTime = LocalDateTime.now();
//            }
            return entry;
        }
    }


    public long getCdcLogId() {
        return cdcLogId;
    }

    public void setCdcLogId(long cdcLogId) {
        this.cdcLogId = cdcLogId;
    }

    public String getInstructionType() {
        return instructionType;
    }

    public void setInstructionType(String instructionType) {
        this.instructionType = instructionType;
    }

    public String getSourceTime() {
        return sourceTime;
    }

    public void setSourceTime(String sourceTime) {
        this.sourceTime = sourceTime;
    }

    public String getTargetTime() {
        return targetTime;
    }

    public void setTargetTime(String targetTime) {
        this.targetTime = targetTime;
    }

    public String getInsertTime() {
        return insertTime;
    }

    public void setInsertTime(String insertTime) {
        this.insertTime = insertTime;
    }

    public String getLogDetail() {
        return logDetail;
    }

    public void setLogDetail(String logDetail) {
        this.logDetail = logDetail;
    }

    public String getSourceDataSourceId() {
        return sourceDataSourceId;
    }

    public void setSourceDataSourceId(String sourceDataSourceId) {
        this.sourceDataSourceId = sourceDataSourceId;
    }

    public String getTargetDataSourceId() {
        return targetDataSourceId;
    }

    public void setTargetDataSourceId(String targetDataSourceId) {
        this.targetDataSourceId = targetDataSourceId;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getResultDetail() {
        return resultDetail;
    }

    public void setResultDetail(String resultDetail) {
        this.resultDetail = resultDetail;
    }

    public Integer getResultCode() {
        return resultCode;
    }

    public void setResultCode(Integer resultCode) {
        this.resultCode = resultCode;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getKafkaEnqueueTime() {
        return kafkaEnqueueTime;
    }

    public void setKafkaEnqueueTime(String kafkaEnqueueTime) {
        this.kafkaEnqueueTime = kafkaEnqueueTime;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    public void setRawMessage(String rawMessage) {
        this.rawMessage = rawMessage;
    }

    public String getSourceSchemaName() {
        return sourceSchemaName;
    }

    public void setSourceSchemaName(String sourceSchemaName) {
        this.sourceSchemaName = sourceSchemaName;
    }
}
