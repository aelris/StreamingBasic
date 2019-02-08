package com.epam.streaming

import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future
import scala.io.BufferedSource

object KafkaProd extends App {
  implicit val executor =  scala.concurrent.ExecutionContext.global
  val topic = util.Try(args(0)).getOrElse("StreamingTopic")
  println(s"Connecting to $topic")

  var producer = KafkaConf.getProducer

/*TODO!*/

   val stream: BufferedSource = scala.io.Source.fromFile(args{0})

     val result = for (line <- stream.getLines.map{ s => Future{s }.map{ s=>s} }.toIndexedSeq) {
        println(s"send -> $line")
        val record: ProducerRecord[Integer, Future[String]] = new ProducerRecord(topic, 1, line)
       producer.send(record)
      }

  producer.close()
}
