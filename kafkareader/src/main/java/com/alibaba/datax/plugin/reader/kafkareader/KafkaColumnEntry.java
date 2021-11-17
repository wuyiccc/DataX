package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * @author wuyiccc
 * @date 2021/11/17 10:43
 */
public class KafkaColumnEntry {

    private Integer index;
    private String type;
    private String value;
    private String name;
    private String format;
    private DateFormat dateParse;

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
        if (StringUtils.isNotBlank(this.format)) {
            this.dateParse = new SimpleDateFormat(this.format);
        }
    }

    public DateFormat getDateParse() {
        return dateParse;
    }

    public void setDateParse(DateFormat dateParse) {
        this.dateParse = dateParse;
    }

    public String toJSONString() {
        return KafkaColumnEntry.toJSONString(this);
    }

    public static String toJSONString(KafkaColumnEntry kafkaColumnEntry) {
        return JSON.toJSONString(kafkaColumnEntry);
    }

}
