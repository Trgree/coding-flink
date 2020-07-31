package org.ace.flink.test

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * nc -l -p 9999
  * 温度在上升，10秒后，告警
  * 使用KeyedProcessFunction和状态管理实现
  */
object ProcessTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("localhost", 9999)
    val result = lines.map(l => {
      val arr = l.split(" ")
      TempData(arr(0).trim(), arr(1).trim().toDouble)
    })
      .keyBy(_.sensor)
      //        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TempData](Time.seconds(1)) {
      //          override def extractTimestamp(t: TempData): Long = t.temp * 1000
      //        })
      .process(new TempAlertProcess())

    lines.print("source data")
    result.print("alert data")
    env.execute("ProcessTest")
  }
}

case class TempData(sensor: String, temp: Double)

class TempAlertProcess extends KeyedProcessFunction[String, TempData, String] {

  //  保存上一条记录的温度
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor("preTemp", classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor("currTime", classOf[Long]))

  override def processElement(tempData: TempData, ctx: KeyedProcessFunction[String, TempData, String]#Context, out: Collector[String]): Unit = {
    val preTemp = lastTemp.value() // 上一个温度
    lastTemp.update(tempData.temp)
    val currTimeTs = currTimer.value()
    // 温度上升且没有注册过定时器
    if (tempData.temp > preTemp && currTimeTs == 0) {
      // 注册一个10秒的定时器
      val timerTs = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currTimer.update(timerTs)
    } else if (tempData.temp <= preTemp || preTemp == 0) {
      // 如果温度下降或第一条记录，删除定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer(currTimeTs)
      currTimer.clear()
    }

  }

//  定义定时器后，在指定时后会回调到这里
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, TempData, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("温度上升：" + ctx.getCurrentKey)
    currTimer.clear()
  }
}