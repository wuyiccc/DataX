
package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class KafkaWriter extends Writer {


    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            // 得到配置
            this.originalConfig = super.getPluginJobConf();

            System.out.println(this.originalConfig.toString());
            System.out.println("go into kafka writer");

        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

        private Configuration taskConfig;

        private boolean print;


        private long recordNumBeforSleep;
        private long sleepTime;

        private Producer producer;

        @Override
        public void init() {
            this.taskConfig = getPluginJobConf();

            this.print = this.taskConfig.getBool(Key.PRINT, true);

            this.recordNumBeforSleep = this.taskConfig.getLong(Key.RECORD_NUM_BEFORE_SLEEP, 0);
            this.sleepTime = this.taskConfig.getLong(Key.SLEEP_TIME, 0);

            String servers = this.taskConfig.getString(Key.SERVERS);
            Properties props = new Properties();
            props.put("bootstrap.servers", servers);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            this.producer = new KafkaProducer<>(props);

            if (recordNumBeforSleep < 0) {
                throw DataXException.asDataXException(KafkaWriterErrorCode.CONFIG_INVALID_EXCEPTION, "recordNumber 不能为负值");
            }
            if (sleepTime < 0) {
                throw DataXException.asDataXException(KafkaWriterErrorCode.CONFIG_INVALID_EXCEPTION, "sleep 不能为负值");
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            LOG.info("#### start kafkawrite...");

            try {
                Record record;

                // 从reader读取到数据
                while ((record = recordReceiver.getFromReader()) != null) {
                    LOG.info("#### kafkawriter get one message: {}", record.toString());
                    if (this.print) {
                        writeToKafka(recordToString(record, this.taskConfig));
                    } else {
                        /* do nothing */
                    }
                }

            } catch (Exception e) {

                throw DataXException.asDataXException(KafkaWriterErrorCode.RUNTIME_EXCEPTION, e);

            }
        }

        // 写入kafka
        private void writeToKafka(String recordString) {
            // 初始化producer相关配置
            this.taskConfig = super.getPluginJobConf();

            String topic = this.taskConfig.getString(Key.TOPIC);

            LOG.info("#### kafkawriter send {} to kafka...", recordString);
            // 发送到kafka通道
            this.producer.send(new ProducerRecord<String, String>(topic, "kafkawriter", recordString));
        }


        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        // 将record组织成[{"value":"hadh5" , "type":"string"} , {"value":324 , "type":"long"}]的string
        private String recordToString(Record record, Configuration writerSliceConfig) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }

            List<KafkaColumnEntry> columnList = getListColumnEntry(writerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
            Map<String, Object> kafkaMessageMap = new HashMap<>();
            if (columnList != null) {
                for (int i = 0; i < columnList.size(); i++) {
                    KafkaColumnEntry kafkaColumnEntry = columnList.get(i);
                    String name = kafkaColumnEntry.getName();
                    Object value = record.getColumn(i).getRawData();
                    kafkaMessageMap.put(name, value);
                }
            }
            return JSON.toJSONString(kafkaMessageMap);
        }

        public static List<KafkaColumnEntry> getListColumnEntry(Configuration configuration, final String path) {
            List<JSONObject> lists = configuration.getList(path, JSONObject.class);
            if (lists == null) {
                return null;
            }
            List<KafkaColumnEntry> result = new ArrayList<>();
            for (final JSONObject object : lists) {
                KafkaColumnEntry kafkaColumnEntry = JSON.parseObject(object.toJSONString(), KafkaColumnEntry.class);
                result.add(kafkaColumnEntry);
            }
            return result;
        }
    }


}
