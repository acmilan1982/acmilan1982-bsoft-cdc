package com.bsoft.common.bean;

import java.util.Objects;

/**
 * ClassName: DDLBuildContext
 * Package:   com.bsoft.common.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/20 - 17:52
 */
public class DDLBuildContext {



    public enum BizAttrMode {
        SYSTEM_DEFAULT,  // 系统级
        TABLE_OVERRIDE   // 表级
    }

    // doris建表属性 来源
    private final BizAttrMode bizAttrMode;
    private final DdlBizAttr bizAttrOverride; // TABLE_OVERRIDE时必须非空

    private DDLBuildContext(BizAttrMode bizAttrMode, DdlBizAttr bizAttrOverride) {
        this.bizAttrMode = Objects.requireNonNull(bizAttrMode, "bizAttrMode");
        this.bizAttrOverride = bizAttrOverride;
        if (bizAttrMode == BizAttrMode.TABLE_OVERRIDE && bizAttrOverride == null) {
            throw new IllegalArgumentException("TABLE_OVERRIDE mode requires non-null bizAttrOverride");
        }
    }

    public static DDLBuildContext systemDefault() {
        return new DDLBuildContext(BizAttrMode.SYSTEM_DEFAULT, null);
    }

    public static DDLBuildContext tableOverride(DdlBizAttr bizAttrOverride) {
        return new DDLBuildContext(BizAttrMode.TABLE_OVERRIDE, bizAttrOverride);
    }

    public boolean isSystemDefault() {
        return bizAttrMode == BizAttrMode.SYSTEM_DEFAULT;
    }

    public boolean isTableOverride() {
        return bizAttrMode == BizAttrMode.TABLE_OVERRIDE;
    }

    public BizAttrMode getBizAttrMode() {
        return bizAttrMode;
    }

    public DdlBizAttr getBizAttrOverride() {
        return bizAttrOverride;
    }
}