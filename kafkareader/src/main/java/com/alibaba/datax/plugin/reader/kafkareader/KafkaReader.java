package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
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
import java.util.regex.Pattern;

public class KafkaReader extends Reader {

	public static class Job extends Reader.Job {

	    private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
	    private Pattern mixupFunctionPattern;
		private Configuration originalConfig;
		private KafkaConsumer consumer;
		// 通过RecordSender往channel写入数据
		public void startRead(RecordSender recordSender) {
			Record record=recordSender.createRecord();
			record.addColumn(new LongColumn(1));
			record.addColumn(new StringColumn("hello,world!"));
			recordSender.sendToWriter(record);
			recordSender.flush();
		}


		@Override
		public void init() {
			System.out.println("go into kafka");
			// 得到参数
			this.originalConfig = super.getPluginJobConf();
			// 从参数中得到配置信息
			// warn: 忽略大小写
			this.mixupFunctionPattern = Pattern.compile(Constant.MIXUP_FUNCTION_PATTERN, Pattern.CASE_INSENSITIVE);
		}



		@Override
		public void prepare() {
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			List<Configuration> configurations = new ArrayList<Configuration>();
			for (int i = 0; i < adviceNumber; i++) {
				configurations.add(this.originalConfig.clone());
			}
			return configurations;
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

	}

	public static class Task extends Reader.Task {

		private Configuration readerSliceConfig;


		private boolean haveMixupFunction;

		private KafkaConsumer consumer;

		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();

			// 设置kafka参数
			String servers = this.readerSliceConfig.getString(Key.SERVERS);
			String groupid = this.readerSliceConfig.getString(Key.GROUPID);
			String topic = this.readerSliceConfig.getString(Key.TOPIC);

			System.out.println("参数: ");
            System.out.println(servers);
            System.out.println(groupid);
            System.out.println(topic);

            Properties props = new Properties();

			props.put("bootstrap.servers", servers);
			props.put("group.id", groupid);
			props.put("enable.auto.commit", "true");
			props.put("auto.commit.interval.ms", "1000");
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(Arrays.asList(topic));

            this.haveMixupFunction = this.readerSliceConfig.getBool(Constant.HAVE_MIXUP_FUNCTION, false);
		}

		@Override
		public void prepare() {
		}

		private void kafkaConsume(RecordSender recordSender){


			// 线程池 接收exit停止
			final boolean[] exitFlag = {false};
			ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
			cachedThreadPool.execute(new Runnable() {
				@Override
				public void run() {
					while (true) {
						Scanner scanner = new Scanner(System.in);
						if(scanner.nextLine().equals("exit")){
							exitFlag[0] = true;
							break;
						}
					}
				}
			});

			ConsumerRecords<String, String> kafkaRecords;
//			boolean isclear = this.readerSliceConfig.getBool(Key.ISCLEAR);

			// 如果之前输入kafka的消息格式错误，则需要清除

				// Kafka循环取得消息
				while (true) {
					kafkaRecords = this.consumer.poll(100);

					//
					for (ConsumerRecord<String, String> kafkarecord : kafkaRecords) {
						System.out.printf("offset = %d, key = %s, value = %s%n", kafkarecord.offset(), kafkarecord.key(), kafkarecord.value());

						// 构建一个datax的record ，value应为json结构
						try {
							Record oneRecord = buildOneRecord(recordSender, kafkarecord.value());
							// 发送给writer
							System.out.println("send to writer " + oneRecord.toString());
							recordSender.sendToWriter(oneRecord);
						}catch (Exception e){
							this.consumer.commitAsync();
							throw DataXException.asDataXException(KafkaReaderErrorCode.ILLEGAL_VALUE,
									"构造一个record失败.", e);
						}

					}
					if(exitFlag[0]){
						System.out.println("out loop");
						break;
					}


			}


		}

		@Override
		// 读数据
		public void startRead(RecordSender recordSender) {

			// 启动一个kafka消费者线程，设置参数，ip，端口，topic
			kafkaConsume(recordSender);
			System.out.println("go out consumer");

		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

		// 构建一列
		private Column buildOneColumn(Configuration eachColumnConfig,String key,String value) throws Exception {

			// 每列的value
//		    String columnValue = eachColumnConfig.getString(Constant.VALUE);
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
                    return new StringColumn(RandomStringUtils.randomAlphanumeric((int)RandomUtils.nextLong(param1Int, param2Int + 1)));
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
                        return new BoolColumn(randomInt <= param1Int ? false : true);
                    }
                } else {
                    return new BoolColumn("true".equalsIgnoreCase(columnValue) ? true : false);
                }
            case BYTES:
                if (isColumnMixup) {
                    return new BytesColumn(RandomStringUtils.randomAlphanumeric((int)RandomUtils.nextLong(param1Int, param2Int + 1)).getBytes());
                } else {
                    return new BytesColumn(columnValue.getBytes());
                }
            default:
                // in fact,never to be here
                throw new Exception(String.format("不支持类型[%s]",
                        columnType.name()));
            }
		}

		private Record buildOneRecord(RecordSender recordSender, String recordJson) {
			if (null == recordSender) {
				throw new IllegalArgumentException(
						"参数[recordSender]不能为空.");
			}

			if (null == recordJson || recordJson.isEmpty()) {
				throw new IllegalArgumentException(
						"record [recordJson]不能为空.");
			}

			Record record = recordSender.createRecord();
			// TODO: JSON解析kafka消息
			try {
				// 解析json
				// [{"value":"data","type":"string"}]
				List<Map> mapList = JSON.parseArray(recordJson,Map.class);

				for (Map mapEntry : mapList) {
					String type = mapEntry.get("type").toString();
					String value = mapEntry.get("value").toString();
//					String conf = String.format("{\"%s\":\"%s\"}", entry.getKey(),entry.getValue());
					Configuration eachColumnConfig = Configuration.from(recordJson);
					// 给record添加新的一行
					record.addColumn(this.buildOneColumn(eachColumnConfig, type, value));
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(KafkaReaderErrorCode.ILLEGAL_VALUE,
						"构造一个record失败.", e);
			}
			return record;
		}
	}

	private enum Type {
		STRING, LONG, BOOL, DOUBLE, DATE, BYTES, ;

		private static boolean isTypeIllegal(String typeString) {
			try {
				Type.valueOf(typeString.toUpperCase());
			} catch (Exception e) {
				return false;
			}

			return true;
		}
	}




}
