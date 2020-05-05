package org.ace.flink.source

import java.util.concurrent.TimeUnit

import org.ace.flink.pojo.TempData
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class AutoGenerateSource extends SourceFunction[TempData]{

  var running = true
  var machines = List("machine1", "machine2", "machine3")
  val rand = new Random()

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[TempData]): Unit = {
    while(running){
      val machine = machines(rand.nextInt(machines.size))
      ctx.collect(TempData(machine, System.currentTimeMillis(), rand.nextInt(100)))
      TimeUnit.SECONDS.sleep(1)
    }
  }
}
