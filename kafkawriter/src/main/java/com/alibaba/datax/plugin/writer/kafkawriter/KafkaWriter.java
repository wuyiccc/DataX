
package com.alibaba.datax.plugin.writer.kafkawriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
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

        private Configuration writerSliceConfig;

        private boolean print;


        private long recordNumBeforSleep;
        private long sleepTime;

        private Producer producer;
        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();

            this.print = this.writerSliceConfig.getBool(Key.PRINT, true);

            this.recordNumBeforSleep = this.writerSliceConfig.getLong(Key.RECORD_NUM_BEFORE_SLEEP, 0);
            this.sleepTime = this.writerSliceConfig.getLong(Key.SLEEP_TIME, 0);

            String servers = this.writerSliceConfig.getString(Key.SERVERS);
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

            if(recordNumBeforSleep < 0) {
                throw DataXException.asDataXException(KafkaWriterErrorCode.CONFIG_INVALID_EXCEPTION, "recordNumber 不能为负值");
            }
            if(sleepTime <0) {
                throw DataXException.asDataXException(KafkaWriterErrorCode.CONFIG_INVALID_EXCEPTION, "sleep 不能为负值");
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
                System.out.println("start write");

                try {
                    Record record;

                    // 从reader读取到数据
                    while ((record = recordReceiver.getFromReader()) != null) {
                        System.out.println("kafkawriter  读取到一条数据 :\n"+ record);
                        if (this.print) {
                            writeToKafka(recordToString(record));
                        } else {
                    /* do nothing */
                        }
                    }

                } catch (Exception e) {

                    throw DataXException.asDataXException(KafkaWriterErrorCode.RUNTIME_EXCEPTION, e);

                }
        }

        // 写入kafka
        private void writeToKafka(String recordString){
            // 初始化producer相关配置
            this.writerSliceConfig = super.getPluginJobConf();

            String topic = this.writerSliceConfig.getString(Key.TOPIC);

            System.out.println("send to kakfa " + topic+ " ");
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
        private String recordToString(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }
            Column column;
            List dataList = new ArrayList();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                Map tmpmap = new HashMap<>();
                tmpmap.put("value",column.getRawData());
                tmpmap.put("type",column.getType());
                dataList.add(tmpmap);
            }
            String jsonStr = JSON.toJSON(dataList).toString();
            System.out.println(jsonStr);
            return jsonStr;

        }
    }

}
