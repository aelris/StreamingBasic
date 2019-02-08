package com.epam.streaming

import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.BufferedSource

object KafkaProd extends App {
  println(s"Start app")
  implicit val executor = scala.concurrent.ExecutionContext.global
  val topic = "StreamingTopic"
  println(s"Connecting to $topic")

  var producer = KafkaConf.getProducer

  /*TODO!*/

  val stream: BufferedSource = scala.io.Source.fromFile(args {
    0
  })

  private val fLine = stream
    .getLines
//    .toList
    .sliding(5, 5)
    .map { lines: Seq[String] =>
      val s = lines.map { line =>
        Future {
          val producer = KafkaConf.getProducer
          val p: ProducerRecord[Integer, String] = new ProducerRecord(topic, 1, line)
          producer.send(p)
        }

      }
      Await.result(Future.sequence(s), 10 seconds)
      println("batch exist")
    }

  println(s"before sleap")
  Thread.sleep(60000)
  println(s"End app")

}
