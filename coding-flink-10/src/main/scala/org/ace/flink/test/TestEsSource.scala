package org.ace.flink.test

import com.mnubo.flink.streaming.connectors.DataRow
import com.mnubo.flink.streaming.connectors.elasticsearch.ElasticsearchDataset
import org.apache.flink.api.scala._


object TestEsSource {

  def main(args: Array[String]): Unit = {
    val esIndexName = "myuser"

    val esNodeHostNames = Set("localhost")

    val esHttpPort = 9200

    val esQuery = """{"fields": ["UUID","FuncName"]}"""

    val dataSet = ElasticsearchDataset.fromElasticsearchQuery[DataRow](
      ExecutionEnvironment.getExecutionEnvironment,
      esIndexName,
      esQuery,
      esNodeHostNames,
      esHttpPort
    )

    dataSet.print()

  }
}
