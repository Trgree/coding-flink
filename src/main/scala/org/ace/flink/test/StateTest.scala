package org.ace.flink.test

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * nc -l -p 9999
  * 温度比上一温度上升一定大小，告警
  * 使用KeyedProcessFunction和状态管理实现
  */
object StateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("localhost", 9999)
    val datastream = lines.map(l => {
      val arr = l.split(" ")
      TempData(arr(0).trim(), arr(1).trim().toDouble)
    })
      .keyBy(_.sensor)
      //        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TempData](Time.seconds(1)) {
      //          override def extractTimestamp(t: TempData): Long = t.temp * 1000
      //        })
      // 使用map flatMap也可以
    datastream .process(new TempChangeAlertProcess(10)).print("alert data")

    // 或直接使用flatMapWithState
    datastream.flatMapWithState[(String, Double, Double), Double]{
      // 如果没有状态，也就是第一条数据，当前温度存入状态，不输出(返回List.empty)
      case (input: TempData, None) => (List.empty, Some(input.temp))
      // 有状态，对比温度差
      case (input: TempData, lastTemp: Some[Double]) => {
        val dif = input.temp - lastTemp.get
        if(dif > 10){
          // 输出元组(放入一个list,如果多个输出，可存入多个元组)，并保存状态Some(input.temp)
          (List((input.sensor, input.temp, lastTemp.get)), Some(input.temp))
        } else {
          (List.empty,  Some(input.temp))
        }
      }
    }
      .print("alert data-flatMap")

    lines.print("source data")

    env.execute("ProcessTest")
  }
}


class TempChangeAlertProcess(offset: Double) extends KeyedProcessFunction[String, TempData, (String, Double, Double)] {

  //  保存上一条记录的温度,也可以直接ValueState[Double]，不过会有默认值0.0，不符合逻辑
  lazy val lastTemp: ValueState[Option[Double]] = getRuntimeContext.getState(new ValueStateDescriptor("preTemp", classOf[Option[Double]]))

  override def processElement(tempData: TempData, ctx: KeyedProcessFunction[String, TempData, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    val preTemp = lastTemp.value() // 上一个温度
    lastTemp.update(Some(tempData.temp))
    if( null != preTemp){
      if(tempData.temp -  preTemp.get > offset){
        out.collect((tempData.sensor, tempData.temp, preTemp.get))
      }
    }

  }


}