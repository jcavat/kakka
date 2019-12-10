package ch.hepia.kakka


import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, Outlet, scaladsl}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Sink, Source, Zip, ZipWith, ZipWithN}

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

object AkkaQuickstart extends App {


  implicit val sys = ActorSystem("MyTest")


  val source: Source[Int, NotUsed] = Source(LazyList.continually(new Random().nextInt(100)).take(1000))
  val maxChunk: Int = 30

  def chunk(n: Int): Flow[Int, Seq[Int], NotUsed] = Flow[Int].sliding(n)
  def chunked: Flow[Int, Seq[Int], NotUsed] = chunk(maxChunk)

  val windowedShort: Flow[Int, Seq[Int], NotUsed] = chunked.map( _.takeRight(10) )
  val windowedLong: Flow[Int, Seq[Int], NotUsed] = chunked

  val sink = Sink.foreach[String]( println )

  val mean: Flow[Seq[Int], Double, NotUsed] = Flow[Seq[Int]].map( s => {
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
      val zip = builder.add(ZipWith[Double,Double, (Double,Double)]{ case (a,b) => (a,b)})
      val ms = builder.add(Flow[String])

      broadcast.out(0) ~> windowedShort.via(mean).async ~> zip.in0
      broadcast.out(1) ~> windowedLong.via(mean).async ~> zip.in1
      zip.out ~> means ~> ms

      FlowShape(broadcast.in, ms.out)

  })
  source.via(graph).runWith(sink)

}
