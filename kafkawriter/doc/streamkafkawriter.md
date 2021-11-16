# DataX KafkaWriter 说明

------------

## 1 快速介绍

KafkaWriter提供了向kafka一个topic写入流数据的功能。服务的用户主要在于DataX开发、测试同学。

**写入kafkatopic内容是一条条结构化数据。**


## 2 功能与限制

KafkaWriter实现了从DataX协议转为Kafka流数据的功能，KafkaWriter如下几个方面约定:

1. 支持且仅支持以kafka的producer形式写入 Kafka中。

7. 支持结构化数据格式

我们不能做到：

1. 同时向多个topic写入数据


## 3 功能说明

### 3.1 配置样例



  ##### 1 从mysql中读取数据，写入kafka

  jobmysqltokafka.json

  ``` json
  {
      "job":{
          "content":[
              {
                  "reader":{
                      "name":"mysqlreader",
                      "parameter":{
                          "column":[
                              "name",
                              "sex",
                              "age"
                          ],
                          "connection":[
                              {
                                  "jdbcUrl":[
                                      "jdbc:mysql://localhost:3306/test"
                                  ],
                                  "table":[
                                      "testTable"
                                  ]
                              }
                          ],
                          "password":"root",
                          "username":"root"
                      }
                  },
                  "writer": {
                      "name": "streamkafkawriter",
                      "parameter": {
  						"topic" : "testdata2",
  						"servers" : "localhost:9092",
                          "print": true,
                          "encoding": "UTF-8"
                      }
                  }
              }
          ],
          "setting":{
              "speed":{
                  "channel":"1"
              }
          }
      }
  }
  ```

  ##### 2 从kafka中读取数据，写入kafka另外一个topic	
  jobkafka.json
  ``` json
  {
    "job": {
        "setting": {
            "speed": {
                "byte":10485760
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "streamkafkareader",
                    "parameter": {
  				
  					"topic" : "testdata",
  					"groupid" : "datax",
  					"servers" : "localhost:9092",
             
                    }
                },
                "writer": {
                    "name": "kafkawriter",
                    "parameter": {
  					"topic" : "testdata2",
  					"servers" : "localhost:9092",
                        "print": true,
                        "encoding": "UTF-8"
                    }
                }
            }
        ]
    }
  }
  ```
### 3.2 参数说明

* **topic**

		* 描述：StreamKafkaWriter写入kafka的topic<br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **servers**

   * 描述：kafka的server的地址端口<br />

   * 必选：是 <br />

   * 默认值：无 <br />



### 3.3 例子展示

##### 1 从mysql中读取数据，写入kafka

```shell
python datax.py jobmysqltokafka.json
```

mysql中原数据

![52620885608](../../img/mysqldata1.png)

写入kafka的数据

![52620894640](../../img/kafkadata1.png)

##### 2   从kafka中读取数据，写入kafka另外一个topic

```shell
python datax.py jobkafka.json
```

同理，这里需要使用exit停止kafka监听，此配置文件从topic（testdata）中读取数据，传入topic（testdata2）中。

向testdata中发送数据

![52621003576](../../img/cmdData2.png)

从testdata2中接收数据

![52621006932](../../img/kafkadata2.png)

## 4 性能报告


## 5 约束限制

略

## 6 FAQ

略


