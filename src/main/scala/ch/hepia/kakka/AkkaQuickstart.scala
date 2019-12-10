package ch.hepia.kakka


import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, Outlet}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source, Zip}

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

object AkkaQuickstart extends App {


  implicit val sys = ActorSystem("MyTest")


  val source: Source[Int, NotUsed] = Source(1 to 1000)
  val windowedShort: Flow[Int, Seq[Int], NotUsed] = Flow[Int].sliding(100).map( _.takeRight(10) )
  val windowedLong: Flow[Int, Seq[Int], NotUsed] = Flow[Int].sliding(100)

  val mean: Flow[Seq[Int], Double, NotUsed] = Flow[Seq[Int]].map( s => {
    println(s)
    s.sum.toDouble / s.length.toDouble
  } )

  val means: Flow[(Double,Double), String, NotUsed] = Flow[(Double,Double)].map {
    case (m1, m2) =>
      s"""
        |moyenne courte $m1
        |moyenne longue $m2
      """.stripMargin
  }


//  source.via(windowedShort).runForeach( seqs => println(seqs) )

  val graph = Flow.fromGraph(GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[Int](2))
      val zip = builder.add(Zip[Double,Double]())
      val ms = builder.add(Flow[String])

      broadcast.out(0) ~> windowedShort ~> mean ~> zip.in0
      broadcast.out(1) ~> windowedLong ~> mean ~> zip.in1
      zip.out ~> means ~> ms

      FlowShape(broadcast.in, ms.out)

  })
  graph.runWith(source, Sink.foreach[String]( println ))

}
