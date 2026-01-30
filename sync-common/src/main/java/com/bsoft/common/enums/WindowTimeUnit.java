package com.bsoft.common.enums;

/**
 * ClassName: WindowTimeUnit
 * Package:   com.bsoft.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/7 - 16:56
 */


/**
 * 窗口长度枚举类型
 */
public enum WindowTimeUnit {

    MINUTE("minute", 60),
    HOUR("hour", 3600),
    DAY("day", 86400);

    private final String name;
    private final int secondsValue;

    WindowTimeUnit(String name, int secondsValue) {
        this.name = name;
        this.secondsValue = secondsValue;
    }

    public String getName() {
        return name;
    }

    public int getSecondsValue() {
        return secondsValue;
    }

    /**
     * 根据秒数获取对应的窗口时间单位
     */
    public static WindowTimeUnit fromSeconds(int seconds) {
        for (WindowTimeUnit unit : values()) {
            if (unit.getSecondsValue() == seconds) {
                return unit;
            }
        }
        throw new IllegalArgumentException("不支持的秒数值: " + seconds);
    }

    @Override
    public String toString() {
        return String.format("%s(%d秒)", name, secondsValue);
    }
}