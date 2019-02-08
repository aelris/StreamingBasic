package com.epam.streaming

import org.apache.kafka.clients.producer.ProducerRecord

import scala.io.BufferedSource

object KafkaProd extends App {
  println(s"Start app")
  implicit val executor = scala.concurrent.ExecutionContext.global
  val topic = util.Try(args(0)).getOrElse("StreamingTopic")
  println(s"Connecting to $topic")

  var producer = KafkaConf.getProducer

  /*TODO!*/

  val stream: BufferedSource = scala.io.Source.fromFile(args {
    0
  })
  private val fLine = stream
    .getLines
    .sliding(5, 5)
    .map { lines: Seq[String] =>
      lines.foreach { line =>

          val producer = KafkaConf.getProducer
          val p: ProducerRecord[Integer, String] = new ProducerRecord(topic, 1, line)
          producer.send(p)

      }
      println("batch exist")
    }.toList
/*
  private val fLine = stream
    .getLines
    .sliding(5, 5)
    .map { lines: Seq[String] =>
      lines.foreach { line =>
        Future {
          val producer = KafkaConf.getProducer
          val p: ProducerRecord[Integer, String] = new ProducerRecord(topic, 1, line)
          producer.send(p)
        }
      }
      println("batch exist")
    }.toList
*/
  println(s"before sleap")
  Thread.sleep(60000)
  println(s"End app")

}
