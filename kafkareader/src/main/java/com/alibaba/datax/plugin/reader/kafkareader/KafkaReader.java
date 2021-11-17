package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wuyiccc
 * @date 2021/11/17 8:43
 */
public class KafkaReader extends Reader {

    /**
     * 数值类型枚举
     */
    private enum Type {
        STRING, LONG, BOOL, DOUBLE, DATE, BYTES,
        ;

        private static boolean isTypeIllegal(String typeString) {
            try {
                Type.valueOf(typeString.toUpperCase());
            } catch (Exception e) {
                return false;
            }

            return true;
        }
    }

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     * Task类init-->prepare-->startRead-->post-->destroy
     * Task类init-->prepare-->startRead-->post-->destroy
     *
     * Job类post-->destroy
     * </pre>
     */
    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration readerOriginConfig;

        /**
         * Job对象初始化工作, 获取配置中的reader部分
         */
        @Override
        public void init() {
            LOG.info("#### kafkareader init() begin...");
            // 获取本插件相关配置
            this.readerOriginConfig = super.getPluginJobConf();
            LOG.info("#### kafkareader init() ok and end...");
        }

        /**
         * 拆分Task
         */
        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("#### kafkareader split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<>();
            for (int i = 0; i < adviceNumber; i++) {
                readerSplitConfigs.add(this.readerOriginConfig.clone());
            }
            return readerSplitConfigs;
        }


        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private static Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConfig;
        private KafkaConsumer consumer;

        /**
         * Task对象初始化
         */
        @Override
        public void init() {
            // 获取split返回的本task相关的配置list中的任何一个
            this.taskConfig = super.getPluginJobConf();

            // 设置kafka参数
            String servers = this.taskConfig.getString(Key.SERVERS);
            String groupId = this.taskConfig.getString(Key.GROUPID);
            String topic = this.taskConfig.getString(Key.TOPIC);

            LOG.info("#### kafka配置参数: severs:{}, groupId:{}, topic:{}", servers, groupId, topic);

            Properties props = new Properties();
            props.put("bootstrap.servers", servers);
            props.put("group.id", groupId);
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("#### kafkareader start...");
            // 启动一个kafka消费者线程，设置参数，ip，端口，topic
            kafkaConsume(recordSender, this.taskConfig);
            LOG.info("#### end kafkareader...");
        }

        /**
         * 消费kafka数据
         *
         * @param recordSender
         */
        private void kafkaConsume(RecordSender recordSender, Configuration readerSliceConfig) {


            // 线程池 接收exit停止
            final boolean[] exitFlag = {false};
            ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
            cachedThreadPool.execute(() -> {
                while (true) {
                    Scanner scanner = new Scanner(System.in);
                    if (scanner.nextLine().equals("exit")) {
                        exitFlag[0] = true;
                        break;
                    }
                }
            });

            ConsumerRecords<String, String> kafkaRecords;

            // Kafka循环取得消息
            while (true) {
                // 读取一条kafka消息
                kafkaRecords = this.consumer.poll(100);
                for (ConsumerRecord<String, String> kafkarecord : kafkaRecords) {
                    LOG.info("#### offset = {}, key = {}, value = {}", kafkarecord.offset(), kafkarecord.key(), kafkarecord.value());
                    // 构建一个datax的record ，value应为json结构
                    try {
                        Record oneRecord = buildOneRecord(recordSender, kafkarecord.value(), readerSliceConfig);
                        // 发送给writer
                        LOG.info("#### begin send to writer:{}", oneRecord.toString());
                        recordSender.sendToWriter(oneRecord);
                    } catch (Exception e) {
                        this.consumer.commitAsync();
                        throw DataXException.asDataXException(KafkaReaderErrorCode.ILLEGAL_VALUE,
                                "构造一个record失败.", e);
                    }

                }
                //LOG.info("#### begin flush one record...");
                recordSender.flush();
                if (exitFlag[0]) {
                    System.out.println("out loop");
                    LOG.info("#### end kakfareader...");
                    break;
                }
            }
        }


        /**
         * 构建一个datax的record记录
         *
         * @param recordSender
         * @param kafkaRecordJson
         * @return
         */
        private Record buildOneRecord(RecordSender recordSender, String kafkaRecordJson, Configuration readerSliceConfig) {
            if (null == recordSender) {
                throw new IllegalArgumentException(
                        "参数[recordSender]不能为空.");
            }
            if (null == kafkaRecordJson || kafkaRecordJson.isEmpty()) {
                throw new IllegalArgumentException(
                        "record [recordJson]不能为空.");
            }
            //List<ColumnEntry> column = UnstructuredStorageReaderUtil.getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
            List<KafkaColumnEntry> columnList = getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);

            Record record = recordSender.createRecord();
            try {
                // 解析json
                // {"id": 1, "name": "wuyiccc"}
                LOG.info("#### kafkareader kafkaRecordJson json:{}", kafkaRecordJson);
                Map<String, Object> kafkaJsonMap = JSON.parseObject(kafkaRecordJson);
                for (KafkaColumnEntry kafkaColumnEntry : columnList) {

                    Configuration eachColumnConfig = Configuration.from(kafkaRecordJson);
                    String name = kafkaColumnEntry.getName();
                    String type = kafkaColumnEntry.getType();
                    String value = kafkaJsonMap.get(name).toString();
                    record.addColumn(this.buildOneColumn(eachColumnConfig, type, value));
                }


            } catch (Exception e) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.ILLEGAL_VALUE,
                        "构造一个record失败.", e);
            }
            return record;
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

        /**
         * 构建record的一列数据
         *
         * @param eachColumnConfig
         * @param key
         * @param value
         * @return
         * @throws Exception
         */
        private Column buildOneColumn(Configuration eachColumnConfig, String key, String value) throws Exception {

            String columnValue = value;
            // 每列的类型
            Type columnType = Type.valueOf(key.toUpperCase());

            // 随机生成数据
            String columnMixup = eachColumnConfig.getString(Constant.RANDOM);
            // 生成随机的两个参数
            long param1Int = eachColumnConfig.getLong(Constant.MIXUP_FUNCTION_PARAM1, 0L);
            long param2Int = eachColumnConfig.getLong(Constant.MIXUP_FUNCTION_PARAM2, 1L);

            boolean isColumnMixup = StringUtils.isNotBlank(columnMixup);

            // 根据不同的类型创建column
            switch (columnType) {
                case STRING:
                    if (isColumnMixup) {
                        // 随机生成
                        return new StringColumn(RandomStringUtils.randomAlphanumeric((int) RandomUtils.nextLong(param1Int, param2Int + 1)));
                    } else {
                        return new StringColumn(columnValue);
                    }
                case LONG:
                    if (isColumnMixup) {
                        return new LongColumn(RandomUtils.nextLong(param1Int, param2Int + 1));
                    } else {
                        return new LongColumn(columnValue);
                    }
                case DOUBLE:
                    if (isColumnMixup) {
                        return new DoubleColumn(RandomUtils.nextDouble(param1Int, param2Int + 1));
                    } else {
                        return new DoubleColumn(columnValue);
                    }
                case DATE:
                    SimpleDateFormat format = new SimpleDateFormat(
                            eachColumnConfig.getString(Constant.DATE_FORMAT_MARK, Constant.DEFAULT_DATE_FORMAT));
                    if (isColumnMixup) {
                        return new DateColumn(new Date(RandomUtils.nextLong(param1Int, param2Int + 1)));
                    } else {
                        return new DateColumn(format.parse(columnValue));
                    }
                case BOOL:
                    if (isColumnMixup) {
                        // warn: no concern -10 etc..., how about (0, 0)(0, 1)(1,2)
                        if (param1Int == param2Int) {
                            param1Int = 0;
                            param2Int = 1;
                        }
                        if (param1Int == 0) {
                            return new BoolColumn(true);
                        } else if (param2Int == 0) {
                            return new BoolColumn(false);
                        } else {
                            long randomInt = RandomUtils.nextLong(0, param1Int + param2Int + 1);
                            return new BoolColumn(randomInt > param1Int);
                        }
                    } else {
                        return new BoolColumn("true".equalsIgnoreCase(columnValue));
                    }
                case BYTES:
                    if (isColumnMixup) {
                        return new BytesColumn(RandomStringUtils.randomAlphanumeric((int) RandomUtils.nextLong(param1Int, param2Int + 1)).getBytes());
                    } else {
                        return new BytesColumn(columnValue.getBytes());
                    }
                default:
                    // in fact,never to be here
                    throw new Exception(String.format("不支持类型[%s]",
                            columnType.name()));
            }
        }


        @Override
        public void destroy() {

        }
    }


}
