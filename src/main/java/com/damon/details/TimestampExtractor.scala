package com.damon.details

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * Description: 自定义时间戳和水印  传参：计算等待最大时延
 * @param maxLaggedTime 最大时延
 */
class TimestampExtractor(val maxLaggedTime: Int) extends AssignerWithPunctuatedWatermarks[(String, String, String, String, Long, String)] with Serializable {
  // Event-time 的最大值，初始时为 0L
  var currentMaxTimestamp = 0L

  override def checkAndGetNextWatermark(lastElement: (String, String, String, String, Long, String), extractedTimestamp: Long): Watermark = {
    // 水印时间 = 当前流上的时间戳 - 最大时延
    val watermark = new Watermark(currentMaxTimestamp - maxLaggedTime * 1000L)
    watermark
  }

  // 参数是根据上游 Tuple 决定的
  override def extractTimestamp(element: (String, String, String, String, Long, String), l: Long): Long = {
    // 拿到最近一次的过车时间，作为时间戳
    val timestamp = element._5
    // 判断并保存 evert-time 的最大值
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    timestamp
  }
}
