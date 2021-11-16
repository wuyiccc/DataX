package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KafkaReaderErrorCode implements ErrorCode {
    // 缺失必要的值
    REQUIRED_VALUE("KafkaReader-00", "缺失必要的值"),
    // 值非法
    ILLEGAL_VALUE("KafkaReader-01", "值非法"),
    // 不支持的column类型
    NOT_SUPPORT_TYPE("KafkaReader-02", "不支持的column类型"),;


    private final String code;
    private final String description;

    private KafkaReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
