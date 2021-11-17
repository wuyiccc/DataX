# DataX KafkaReader 说明

------------

## 1 快速介绍

KafkaReader提供了kafka中流数据的能力。在底层实现上，KafkaReader获取kafka对应topic下的数据，并转换为DataX传输协议传递给Writer。

**kafka中的数据应为符合要求的json格式。**


## 2 功能与限制

KafkaReader实现了从kafka中读取数据并转为DataX协议的功能。目前KafkaReader支持功能如下：

1. 支持且仅支持读取符合要求的json格式，例如: {"id":"1", "name":"testname2"}。

   ​

我们暂时不能做到：

1. 在读取的同时会阻塞writer的写入，暂时无法处理

2.  不能同时处理多个topic数据


## 3 功能说明


### 3.1 配置样例

- ##### 1  从kafka中读取数据，写入mysql

  kafka2mysql.json

 ```json
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
          "name": "kafkareader",
          "parameter": {
            "topic" : "testdata",
            "groupid" : "datax",
            "servers" : "192.168.146.17:9092",
            "column": [
              {
                "name": "id",
                "type": "string"
              }, {
                "name": "name",
                "type": "string"
              }
            ]
          }
        },
        "writer": {
          "name":"mysqlwriter",
          "parameter":{
            "column":[
              "id",
              "name"
            ],
            "connection":[
              {
                "jdbcUrl":"jdbc:mysql://192.168.146.18:3307/test2",
                "table":[
                  "testTable"
                ]
              }
            ],
            "password":"123",
            "username":"root"
          }
        }
      }
    ]
  }
} 
 ```

  ​

  ##### 2  从kafka中读取数据，写入kafka另外一个topic

  kafka2kafka.json

  ```json
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
                    "name": "streamkafkawriter",
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

	* 描述：从kafka对应的topic读取数据 <br />

	* 必选：是 <br />
	
	* 默认值：无 <br />

* **groupid**

	* 描述：kafka消费者的groupid <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **servers**

  * 描述：kafka服务端的地址端口。 <br />

  * 必选：是 <br />

  * 默认值：无 <br />

  ​



### 3.3 实例说明

* ##### 1  从kafka中读取数据，写入mysql

  执行以下命令后，会等待kafka通道中的数据，需要停止时，在命令行输入exit并回车，即停止从kafka读取数据

  ```shell
  python datax.py jobkafkatomysql.json
  ```

  使用命令行发送数据到kafka

  ![52620944683](../../img/cmdData.png)

  datax控制台输入exit退出监听

  ![52620961933](../../img/cmddatax.png)

  数据写入mysql

  ![52620948844](../../img/cmdmysqldata.png)

  ​

  ##### 2   从kafka中读取数据，写入kafka另外一个topic

  ```shell
  python datax.py jobkafka.json
  ```

  同理，这里需要使用exit停止kafka监听，此配置文件从topic（testdata）中读取数据，传入topic（testdata2）中。

  向testdata中发送数据

  ![52621003576](../../img/cmdData2.png)

  从testdata2中接收数据

  ![52621006932](../../img/kafkadata2.png)

  ​


## 4 性能报告

略

## 5 约束限制

datax是用于离线数据同步的工具，kafka处理的是实时流数据，所以当以kafka作为数据源时，必须要等待kafkareader将需要的数据从kafka中读取完毕，才能使用writer插件写入要导入的数据库或文件系统。这里的暂时没有实现边读边写。

## 6 FAQ

略


