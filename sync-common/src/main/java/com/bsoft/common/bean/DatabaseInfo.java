package com.bsoft.common.bean;

import com.bsoft.common.enums.DatabaseType;

import java.io.Serializable;
import java.util.Objects;

/**
 * ClassName: DatabaseInfo
 * Package:   com.bsoft.common.bean
 * Description: 数据库信息Bean,包含数据库类型和版本
 *
 * @Author: snoopy
 * @Create: 2025/7/23
 */
public class DatabaseInfo implements Serializable {

    private DatabaseType type;

    private String version;

    private Integer majorVersion;

    private Integer minorVersion;

    public DatabaseInfo() {
    }

    public DatabaseInfo(DatabaseType type, String version) {
        this.type = type;
        this.version = version;
    }

    public DatabaseType getType() {
        return type;
    }

    public void setType(DatabaseType type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Integer getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(Integer majorVersion) {
        this.majorVersion = majorVersion;
    }

    public Integer getMinorVersion() {
        return minorVersion;
    }

    public void setMinorVersion(Integer minorVersion) {
        this.minorVersion = minorVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseInfo that = (DatabaseInfo) o;
        return type == that.type && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, version);
    }

    @Override
    public String toString() {
        return "DatabaseInfo{" +
                "type=" + type +
                ", version='" + version + '\'' +
                ", majorVersion=" + majorVersion +
                ", minorVersion=" + minorVersion +
                '}';
    }

}
