--定义数据源
CREATE TABLE sourceTable (
  name STRING,
  age BIGINT
) WITH (
  'connector.type' = 'filesystem',--支持 filesystem/kafka/es/jdbc/hive
  'connector.path' = 'data/test.csv',
  'format.type' = 'csv' --支持 csv/json/avro
);
--定义sink
CREATE TABLE sinkTable (
  name STRING,
  age BIGINT
) WITH (
  'connector.type' = 'filesystem',
  'connector.path' = 'data/sink.csv',
  'format.type' = 'csv'
);
--执行数据处理
insert into sinkTable select name,age from sourceTable where name='tom';
