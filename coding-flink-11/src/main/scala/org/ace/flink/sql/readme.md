#### flink 1.11支持的connector和format

##### Connector

| 类型          | 输入 | 输出 | 描述 |
| ------------- | ---- | ---- | ------------- |
| Kafka         | ✔️ | ✔️ |  |
| JDBC          | ✔️ | ✔️ |  |
| Elasticsearch |      | ✔️ |  |
| FileSystem | ✔️ | ✔️ |  |
| HBase | ✔️ | ✔️ |  |
| DataGen | ✔️ |      | 生成测试数据 |
| Print |      | ✔️ | 打印数据到控制台 |
| BlackHole |      | ✔️ | 黑洞，使用数据消失，一般用于性能测试或使用UDF输出，没有sink时 |

##### format

| Formats                                                      | Supported Connectors                                         |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| [CSV](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/csv.html) | [Apache Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/kafka.html), [Filesystem](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/filesystem.html) |
| [JSON](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/json.html) | [Apache Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/kafka.html), [Filesystem](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/filesystem.html), [Elasticsearch](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/elasticsearch.html) |
| [Apache Avro](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/avro.html) | [Apache Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/kafka.html), [Filesystem](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/filesystem.html) |
| [Debezium CDC](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/debezium.html) | [Apache Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/kafka.html) |
| [Canal CDC](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/canal.html) | [Apache Kafka](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/kafka.html) |
| [Apache Parquet](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/parquet.html) | [Filesystem](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/filesystem.html) |
| [Apache ORC](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/orc.html) | [Filesystem](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/filesystem.html) |





