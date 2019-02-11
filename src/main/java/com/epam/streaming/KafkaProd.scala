package com.epam.streaming

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.BufferedSource

object KafkaProd extends App {
  println(s"Start app")
  implicit val executor = scala.concurrent.ExecutionContext.global
  val topic = args{0}
  println(s"Connecting to $topic")

  var producer = KafkaConf.getProducer

  val stream: BufferedSource = scala.io.Source.fromFile(args{1})

  private val fLine = stream
    .getLines
    .sliding(5,5)
    .map { lines: Seq[String] =>
      val s: Seq[Future[util.concurrent.Future[RecordMetadata]]] = lines.map { line =>
        Future {
          val producer: KafkaProducer[Integer, String] = KafkaConf.getProducer
          val p: ProducerRecord[Integer, String] = new ProducerRecord(topic, 1, line)
          producer.send(p)
        }

      }
      Await.result(Future.sequence(s), 10 seconds)

    }.toList
  println(s"before sleep")
  Thread.sleep(60000)
  println(s"End app")
producer.close()
}
