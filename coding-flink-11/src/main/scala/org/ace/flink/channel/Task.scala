package org.ace.flink.channel

import scala.beans.BeanProperty

/**
 *
 * @author jace
 * @Date 2020/9/1 5:38 下午
 */
case class Task (@BeanProperty var tables: Array[String] = null,
                 @BeanProperty var sqls: Array[String] = null,
                @BeanProperty var functions: Array[String] = null)